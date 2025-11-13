using SistemaDeAnalisis.Interfaces;
using SistemaDeAnalisis.Models;
using SistemaDeAnalisis.Services;

namespace SistemaDeAnalisis.Services
{
    public class ExtractionService
    {
        private readonly ILogger<ExtractionService> _logger;
        private readonly IEnumerable<IExtractor> _extractors;
        private readonly DataLoader _dataLoader;

        public ExtractionService(
            ILogger<ExtractionService> logger,
            IEnumerable<IExtractor> extractors,
            DataLoader dataLoader)
        {
            _logger = logger;
            _extractors = extractors;
            _dataLoader = dataLoader;

            _logger.LogInformation("ExtractionService inicializado con {Count} extractors", _extractors?.Count() ?? 0);
        }

        public async Task ExecuteExtractionAsync()
        {
            _logger.LogInformation(" INICIO DEL PROCESO ETL...");

            var allData = new List<SalesData>();

            try
            {
                _logger.LogInformation("Buscando extractors registrados...");

                if (_extractors == null || !_extractors.Any())
                {
                    _logger.LogError(" NO SE ENCONTRARON EXTRACTORS REGISTRADOS");
                    return;
                }

                _logger.LogInformation("Encontrados {Count} extractors", _extractors.Count());

                foreach (var extractor in _extractors)
                {
                    _logger.LogInformation("Procesando extractor: {ExtractorName}", extractor.GetType().Name);
                    var data = await ExtractFromSourceAsync(extractor);
                    allData.AddRange(data);
                    _logger.LogInformation(" {ExtractorName} completado", extractor.GetType().Name);
                }

                _logger.LogInformation(" TOTAL DE REGISTROS EXTRAÍDOS: {Count}", allData.Count);

                if (allData.Any())
                {
                    _logger.LogInformation("Iniciando carga de datos...");
                    await _dataLoader.LoadDataAsync(allData);
                    _logger.LogInformation(" CARGA DE DATOS COMPLETADA");
                }
                else
                {
                    _logger.LogWarning(" NO HAY DATOS PARA CARGAR");
                }

                _logger.LogInformation(" PROCESO ETL COMPLETADO EXITOSAMENTE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, " ERROR DURANTE EL PROCESO ETL");
            }
        }

        private async Task<IEnumerable<SalesData>> ExtractFromSourceAsync(IExtractor extractor)
        {
            try
            {
                _logger.LogInformation("Ejecutando {ExtractorName}...", extractor.GetType().Name);
                var data = await extractor.ExtractAsync();
                _logger.LogInformation(" {ExtractorName} extrajo {Count} registros",
                    extractor.GetType().Name, data.Count());
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, " ERROR en {ExtractorName}", extractor.GetType().Name);
                return new List<SalesData>();
            }
        }
    }
}