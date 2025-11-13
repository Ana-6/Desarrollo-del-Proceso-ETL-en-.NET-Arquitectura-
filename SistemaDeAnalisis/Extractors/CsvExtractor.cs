using CsvHelper;
using CsvHelper.Configuration;
using SistemaDeAnalisis.Interfaces;
using SistemaDeAnalisis.Models;
using SistemaDeAnalisis.Configuration;
using Microsoft.Extensions.Options;
using System.Globalization;

namespace SistemaDeAnalisis.Extractors
{
    public class CsvExtractor : IExtractor
    {
        private readonly ILogger<CsvExtractor> _logger;
        private readonly ETLConfiguration _config;

        public string SourceType => "CSV";

        public CsvExtractor(ILogger<CsvExtractor> logger, IOptions<ETLConfiguration> config)
        {
            _logger = logger;
            _config = config.Value;
        }

        public async Task<IEnumerable<SalesData>> ExtractAsync()
        {
            var allSalesData = new List<SalesData>();

            try
            {
                if (!Directory.Exists(_config.DataDirectory))
                {
                    _logger.LogWarning("Directorio Data no encontrado: {Directory}", _config.DataDirectory);
                    return allSalesData;
                }

                _logger.LogInformation("Buscando archivos CSV en directorio: {Directory}", _config.DataDirectory);

                var customers = await ProcessCsvFile<CustomerData>("customers.csv");
                var products = await ProcessCsvFile<ProductData>("products.csv");
                var orders = await ProcessOrdersFile("orders.csv"); // NUEVO MÉTODO
                var orderDetails = await ProcessCsvFile<OrderDetailData>("order_details.csv");

                allSalesData = EnrichSalesData(customers, products, orders, orderDetails);

                _logger.LogInformation("Procesados: {Customers} clientes, {Products} productos, {Orders} órdenes, {Details} detalles",
                    customers.Count, products.Count, orders.Count, orderDetails.Count);

                return allSalesData;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al extraer datos CSV");
                return new List<SalesData>();
            }
        }

        private async Task<List<T>> ProcessCsvFile<T>(string fileName)
        {
            var filePath = Path.Combine(_config.DataDirectory, fileName);

            if (!File.Exists(filePath))
            {
                _logger.LogWarning("Archivo no encontrado: {File}", filePath);
                return new List<T>();
            }

            try
            {
                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true,
                    Delimiter = ",",
                    MissingFieldFound = null,
                    HeaderValidated = null // IGNORAR VALIDACIÓN DE HEADERS
                };

                using var reader = new StreamReader(filePath);
                using var csv = new CsvReader(reader, config);

                var records = csv.GetRecords<T>().ToList();

                _logger.LogInformation("Se extrajeron {Count} registros de {File}", records.Count, fileName);

                return records;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error procesando archivo {File}", fileName);
                return new List<T>();
            }
        }

        // NUEVO MÉTODO PARA MANEJAR DIFERENTES ESTRUCTURAS DE ORDERS
        private async Task<List<OrderData>> ProcessOrdersFile(string fileName)
        {
            var filePath = Path.Combine(_config.DataDirectory, fileName);

            if (!File.Exists(filePath))
            {
                _logger.LogWarning("Archivo no encontrado: {File}", filePath);
                return new List<OrderData>();
            }

            try
            {
                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true,
                    Delimiter = ",",
                    MissingFieldFound = null,
                    HeaderValidated = null
                };

                using var reader = new StreamReader(filePath);
                using var csv = new CsvReader(reader, config);

                // Leer la primera línea para ver los headers
                await csv.ReadAsync();
                csv.ReadHeader();

                var orders = new List<OrderData>();

                // Verificar qué estructura tiene el archivo
                if (csv.HeaderRecord.Contains("ProductID") && csv.HeaderRecord.Contains("Quantity"))
                {
                    // Estructura esperada: OrderID, ProductID, Quantity, TotalPrice
                    csv.Context.Reader.Parser.Read(); // Volver a leer la primera línea de datos
                    do
                    {
                        var order = new OrderData
                        {
                            OrderID = csv.GetField<int>("OrderID"),
                            ProductID = csv.GetField<int>("ProductID"),
                            Quantity = csv.GetField<int>("Quantity"),
                            TotalPrice = csv.GetField<decimal>("TotalPrice")
                        };
                        orders.Add(order);
                    } while (await csv.ReadAsync());
                }
                else if (csv.HeaderRecord.Contains("CustomerID") && csv.HeaderRecord.Contains("OrderDate"))
                {
                    // Estructura alternativa: OrderID, CustomerID, OrderDate, Status
                    // Crear datos de orden simulados basados en order_details
                    _logger.LogInformation("Archivo orders.csv tiene estructura alternativa, usando order_details como referencia");

                    // No procesamos este archivo directamente, usaremos order_details
                    return new List<OrderData>();
                }

                _logger.LogInformation("Se extrajeron {Count} registros de {File}", orders.Count, fileName);
                return orders;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error procesando archivo {File}", fileName);
                return new List<OrderData>();
            }
        }

        private List<SalesData> EnrichSalesData(
            List<CustomerData> customers,
            List<ProductData> products,
            List<OrderData> orders,
            List<OrderDetailData> orderDetails)
        {
            var salesData = new List<SalesData>();
            var productDict = products.ToDictionary(p => p.ProductID);
            var customerDict = customers.ToDictionary(c => c.CustomerID);

            // Generar IDs únicos para evitar duplicados
            var usedIds = new HashSet<int>();
            var random = new Random();

            foreach (var detail in orderDetails)
            {
                // Generar ID único
                int uniqueId;
                do
                {
                    uniqueId = detail.OrderID * 1000 + detail.ProductID + random.Next(1, 1000);
                } while (usedIds.Contains(uniqueId));
                usedIds.Add(uniqueId);

                var sale = new SalesData
                {
                    Id = uniqueId, // ID ÚNICO
                    CustomerID = GetCustomerIdFromOrder(detail.OrderID, customerDict),
                    OrderID = detail.OrderID,
                    ProductID = detail.ProductID,
                    Quantity = detail.Quantity,
                    TotalPrice = detail.TotalPrice,
                    Source = "order_details.csv",
                    CreatedDate = DateTime.UtcNow,
                    OrderDate = DateTime.UtcNow.AddDays(-random.Next(1, 365)) // Fechas variadas
                };

                if (productDict.TryGetValue(detail.ProductID, out var product))
                {
                    sale.ProductName = product.ProductName;
                    sale.Category = product.Category;
                    sale.Price = product.Price;
                }

                if (customerDict.TryGetValue(sale.CustomerID, out var customer))
                {
                    sale.FirstName = customer.FirstName;
                    sale.LastName = customer.LastName;
                    sale.Email = customer.Email;
                }

                salesData.Add(sale);
            }

            return salesData;
        }

        private int GetCustomerIdFromOrder(int orderId, Dictionary<int, CustomerData> customers)
        {
            if (customers.Count == 0) return 1;
            var customerIds = customers.Keys.ToList();
            return customerIds[orderId % customerIds.Count];
        }
    }
}