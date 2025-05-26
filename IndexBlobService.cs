using IndexingWorkerContainerApp.Services.Interfaces;
using IndexingWorkerContainerApp.ViewModels;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using VerataShared.BlobStorage;
using VerataShared.ViewModels.PackageGeneration;
using VerataShared.ViewModels.TextSearch;

namespace IndexingWorkerContainerApp.Services
{
   public class IndexBlobService : IIndexBlobService
   {
      private readonly IConfiguration _config;
      private readonly ILogger<IndexBlobService> _logger;
      private readonly IBlobStorage _tokenAuthBlobStorage;
      private readonly IBlobStorage _ocrBlobStorage;
      private readonly TelemetryClient _telemetryClient;
      private readonly IApiCaller _apiCaller;

      public IndexBlobService( IConfiguration config,
         ILogger<IndexBlobService> logger,
         IDictionary<BlobStorageServiceName, IBlobStorage> blobStorageClients,
         IApiCaller apiCaller,
         TelemetryClient telemetryClient )
      {
         _config = config;
         _logger = logger;
         _apiCaller = apiCaller;
         _ocrBlobStorage = blobStorageClients[BlobStorageServiceName.Ocr];
         _tokenAuthBlobStorage = blobStorageClients[BlobStorageServiceName.TokenAuth];
         _telemetryClient = telemetryClient;
      }

      public async Task IndexBlobsForPaRequestAsync( string serviceBusEntityMessage )
      {
         var textSearchRequest = JsonConvert.DeserializeObject<TextSearchRequest>(serviceBusEntityMessage);
         if( textSearchRequest is null )
         {
            _logger.LogError( $"Invaid input [{serviceBusEntityMessage}]" );
            return;
         }

         _logger.LogInformation($"{nameof(IndexBlobsForPaRequestAsync)} - " +
           $"Starting for PaRequestId {textSearchRequest.PaRequestId}");

         using var operation = _telemetryClient.StartOperation<RequestTelemetry>($"{nameof(IndexBlobsForPaRequestAsync)}_RequestOperation");

         operation.Telemetry.Properties.TryAdd(nameof(TextSearchRequest.PaRequestId), textSearchRequest.PaRequestId.ToString());
         
         // TODO In future, this should be happenning in TokenAuth NotifyDocumentsDone API.
         // We are currently doing this here because
         // we are only indexing subset of documents or PA where OCR was performed
         await _apiCaller.CreateBlobIndexingRequestsAsync( textSearchRequest.PaRequestId );

         var indexBlobRequestsForPa = await _apiCaller.GetDocumentsReadyForIndexingAsync( textSearchRequest.PaRequestId );
         if( indexBlobRequestsForPa is null || indexBlobRequestsForPa.Count == 0 )
         {
            _logger.LogInformation( $"Unable to find documents for text search for PaRequestId {textSearchRequest.PaRequestId}" );
            return;
         }

         operation.Telemetry.Properties.TryAdd( "DocumentCount", indexBlobRequestsForPa.Count.ToString() );

         var indexingConfig = new IndexingConfiguration()
         {
            Configuration = _config,
            FilePath      = textSearchRequest.PaRequestId.ToString(),
            Logger        = _logger,
            Blob          = _tokenAuthBlobStorage
         };

         var indexer = new Indexing( indexingConfig );

         try
         {
            var writer = indexer.PrepareWriterAsync();

            var updateIndexingRequestsStatus = new List<UpdateIndexingRequestStatus>();

            foreach ( var indexBlobRequest in indexBlobRequestsForPa )
            {
               var indexRequestStatus = await AddDocumentToLuceneIndexAsync( indexBlobRequest, indexer, writer );
               if( indexRequestStatus is null )
               {
                  continue;
               }

               updateIndexingRequestsStatus.Add( indexRequestStatus );
            }

            using var reader = indexer.PrepareReaderAsync();
            await reader;

            // Get a list of search terms for this PA ordered by usage.
            var terms = indexer.GetSearchTermList();
            indexer.SetLargeField( "Index", terms );

            await _apiCaller.UpdateIndexingRequestsStatusAsync( updateIndexingRequestsStatus );

            _logger.LogInformation($"{nameof(IndexBlobsForPaRequestAsync)} - " +
           $"Text Search successfully completed for PaRequestId {textSearchRequest.PaRequestId}");

            operation.Telemetry.Success = true;
            operation.Telemetry.ResponseCode = AppInsightsTelemetryInitializer.SuccessfulOperation;
         }
         catch ( Exception ex )
         {
            _logger.LogException( ex, $"Unable to run text search on documents for PaRequest : {textSearchRequest.PaRequestId}" );

            operation.Telemetry.Success = false;
            operation.Telemetry.ResponseCode = AppInsightsTelemetryInitializer.FailedOperation;
         }
         finally
         {
            indexer?.Dispose();
         }
      }

      public async Task<UpdateIndexingRequestStatus?> AddDocumentToLuceneIndexAsync( BlobIndexingRequestViewModel indexBlobRequest,
         Indexing indexer, Task writer )
      {
         if ( indexBlobRequest is null )
         {
            _logger.LogError( "Index blob request is invalid" );
            return null;
         }

         _logger.LogInformation( $"{nameof(AddDocumentToLuceneIndexAsync)} - " +
            $"Starting for BlobDocumentId {indexBlobRequest.BlobDocumentId}" );

         var updateIndexingRequest = new UpdateIndexingRequestStatus
         {
            BlobDocumentId = indexBlobRequest.BlobDocumentId,
            Status = AiRequestStatus.NotStarted
         };

         try
         {
            if ( string.IsNullOrEmpty( indexBlobRequest.OcrBlobName ) )
            {
               _logger.LogError( $"OcrBlobName is invalid for BlobDocumentId {indexBlobRequest.BlobDocumentId}" );

               return MarkAsFailed(updateIndexingRequest);
            }

            var ocrText = await _ocrBlobStorage.ReadBlobToString( indexBlobRequest.OcrBlobName );
            if ( string.IsNullOrEmpty( ocrText ) )
            {
               _logger.LogError( $"Ocr text is not found on blob {indexBlobRequest.OcrBlobName}" );

               return MarkAsFailed( updateIndexingRequest );
            }

            ocrText ??= string.Empty;

            // Strip out unnecessary white space chars.
            ocrText = Regex.Replace( ocrText, @"(\n|\r|_|\s)+", " " );

            // Create a new Lucene document.
            var doc = new MedicalRecord
            {
               Id        = indexBlobRequest.BlobDocumentId.ToString(),
               Name      = indexBlobRequest.OcrBlobName,
               FileText  = ocrText,
               TitleText = indexBlobRequest.DocumentDescription ?? string.Empty
            };

            if ( writer != null )
            {
               await writer;
            }

            // Add the document to the Lucene index.
            indexer.AddDocument( doc );

            updateIndexingRequest.Status = AiRequestStatus.Done;
         }
         catch ( Exception ex )
         {
            updateIndexingRequest.Status = AiRequestStatus.Failed;

            _logger.LogException( ex, $"Failed to add document to lucene index. " +
               $"BlobDocumentId : {indexBlobRequest.BlobDocumentId}" );
         }

         updateIndexingRequest.CompletedProcessingUtc = DateTime.UtcNow;

         return updateIndexingRequest;
      }

      private static UpdateIndexingRequestStatus MarkAsFailed( UpdateIndexingRequestStatus request )
      {
         request.Status = AiRequestStatus.Failed;
         request.CompletedProcessingUtc = DateTime.UtcNow;

         return request;
      }
   }
}
