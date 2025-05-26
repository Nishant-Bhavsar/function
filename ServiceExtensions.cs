using IndexingWorkerContainerApp.Code;
using IndexingWorkerContainerApp.Services.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace IndexingWorkerContainerApp.Services
{
    public static class ServiceExtensions
    {
      public static IServiceCollection AddIndexingWorkerServiceExtensions( this IServiceCollection services )
      {
         services.TryAddScoped<IIndexBlobService, IndexBlobService>();
         services.TryAddSingleton<ApiCallerSet>();
         services.TryAddScoped<IApiCaller, ApiCaller>();
         return services;
      }
    }
}
