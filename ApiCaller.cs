using IndexingWorkerContainerApp.Code;
using IndexingWorkerContainerApp.Services.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using VerataShared.CustomExceptions;
using VerataShared.VerataEnvironment;
using VerataShared.ViewModels.TextSearch;

namespace IndexingWorkerContainerApp.Services
{
   public class ApiCaller : IApiCaller
   {
      private readonly ApiCallerSet _apiCallerSet;
      private readonly Uri _tokenAuthUri;
      private readonly ILogger<ApiCaller> _logger;

      public ApiCaller( IConfiguration configuration,
                        IVerataEnvironmentVariable env,
                        ILogger<ApiCaller> logger,
                        ApiCallerSet apiCallerSet )
      {
         _apiCallerSet = apiCallerSet;
         _logger = logger;

         var tokenAutUrl = configuration.GetSection("TokenAuthUrls")
                                        .GetSection(env.EnvironmentName)
                                        .Value;

         if ( string.IsNullOrEmpty(tokenAutUrl) )
         {
            throw new ArgumentNullException($"TokenAuth URL not found for environment: {env.EnvironmentName}",
               nameof(tokenAutUrl));
         }

         _tokenAuthUri = new Uri(tokenAutUrl);
      }

      public async Task CreateBlobIndexingRequestsAsync( long paRequestId )
      {
         var input = new IndexingRequestViewModel
         {
            PaRequestId = paRequestId
         };

         var response = await _apiCallerSet.TokenAuthBaseApiCaller.GenericSendBody(input,
                                                                             _tokenAuthUri,
                                                                             "api/Indexing/CreateBlobIndexingRequests");
         if ( !response.IsSuccessful )
         {
            throw new RestApiCallFailedException($"TokenAuth CreateBlobIndexingRequests API call failed " +
               $"for PaRequestId {paRequestId}, Response Content : [{response.Content}]");
         }
      }

      public async Task<List<BlobIndexingRequestViewModel>> GetDocumentsReadyForIndexingAsync( long paRequestId )
      {
         var response = await _apiCallerSet.TokenAuthBaseApiCaller.Get(_tokenAuthUri,
                                                                    $"api/Indexing/GetDocumentsReadyForIndexing/{paRequestId}");
         if( response is null || !response.IsSuccessful )
         {
            throw new RestApiCallFailedException( $"TokenAuth GetDocumentsReadyForIndexing API call failed for ParequestId : {paRequestId}," +
               $"Response Content : [{response?.Content}]" );
         }

         return JsonConvert.DeserializeObject<List<BlobIndexingRequestViewModel>>( response.Content ?? string.Empty ) ?? [];
      }

      public async Task UpdateIndexingRequestsStatusAsync( List<UpdateIndexingRequestStatus> input )
      {
         if ( input is null )
         {
            throw new Exception($"Input is null {nameof(input)}");
         }

         if( input.Count == 0 )
         {
            _logger.LogInformation("No indexing requests found to update in TokenAuth.");
            return;
         }

         var model = new UpdateIndexingRequestsStatusViewModel
         {
            UpdateIndexingRequestsStatus = input
         };

         var response = await _apiCallerSet.TokenAuthBaseApiCaller.GenericSendBody(model,
                                                                             _tokenAuthUri,
                                                                             "api/Indexing/UpdateIndexingRequestsStatus");
         if ( !response.IsSuccessful )
         {
            throw new RestApiCallFailedException($"TokenAuth UpdateIndexingRequestsStatus API call failed " +
               $", Response Content : [{response.Content}]");
         }
      }
   }
}
