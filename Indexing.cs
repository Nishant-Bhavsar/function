using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using VerataShared.BlobStorage;
using IndexingWorkerContainerApp.Services.Interfaces;
using Lucene.Net.Analysis.Standard;
using IndexingWorkerContainerApp.ViewModels;
using Lucene.Net.Util;
using Microsoft.Extensions.Logging;
using Lucene.Net.Search.Highlight;
using Lucene.Net.Documents;
using Lucene.Net.Search;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Store.Azure;
using LuceneDirectory = Lucene.Net.Store.Directory;
using Lucene.Net.QueryParsers.Classic;

namespace IndexingWorkerContainerApp.Services
{
   public sealed class Indexing : IDisposable, IIndexReader, IIndexWriter
   {
      private readonly StandardAnalyzer      _analyzer;
      private readonly IndexingConfiguration _config;
      private IndexWriter                    _writer;
      private DirectoryReader                _reader;
      private LuceneDirectory                _folder;
      public string RemotePath { get; }

      private const int MAX_WORDS = 25000;

      public Indexing( IndexingConfiguration config )
      {
         _config = config;
         RemotePath = GetRemotePath( config );
         _folder = GetFolder( RemotePath );
         _analyzer = new StandardAnalyzer( LuceneVersion.LUCENE_48 );
      }

      /// <summary>
      /// This method queries the index on the field specified with the search phrase.
      /// </summary>
      /// <typeparam name="T">The object model you want returned.</typeparam>
      /// <param name="field">The field to query on.</param>
      /// <param name="phrase">The search phrase to use to query.</param>
      /// <returns>List[T]</returns>
      public List<T> Query<T>( string field, string phrase ) where T : new()
      {
         List<T> output    = new List<T>();
         var     searcher  = new IndexSearcher( _reader ); // Create an Index Searcher using our reader object.
         var     query     = new QueryParser( LuceneVersion.LUCENE_48, field, _analyzer ).Parse( phrase ); // Generate a Lucene Query expression based on the search phrase array.
         var     hits      = searcher.Search( query, _config.MaxMatches ); // Perform search and get list of hits back.

         hits.ScoreDocs.ToList().ForEach( o => output.Add( GetObjectOfType<T>( searcher.Doc( o.Doc ), query, field, o.Score ) ) );

         return output;
      }

      /// <summary>
      /// This method queries the index on the field specified in the selector and with the search phrase.
      /// </summary>
      /// <param name="selector">The string field to perform the search against.  Searches can only be performed against string types.</param>
      /// <param name="phrase">The query phrase to search for.</param>
      /// <typeparam name="T">The given type of objects we should return.</typeparam>
      /// <returns>List[T]</returns>
      public List<T> Query<T>( Expression<Func<T, dynamic>> selector, string phrase ) where T : new()
      {
         var    field = selector.Body.ToString().Split( '.' )[ 1 ];
         var response = Query<T>( field, phrase);

         return response;
      }

      /// <summary>
      /// This method queries the entire contents of the index.
      /// </summary>
      /// <typeparam name="T">The given type of objects we should return.</typeparam>
      /// <returns>List[T]</returns>
      public List<T> Query<T>() where T : new()
      {
         List<T> output = new List<T>();
         int     max    = _reader.NumDocs;

         // Build a list of PAs that we want to keep in the index.
         for ( int i = 0; i < max; i++ )
         {
            var doc = GetObjectOfType<T>( _reader.Document( i ), null, null, 0 );
            output.Add( doc );
         }

         return output;
      }

      /// <summary>
      /// Gets a list of search terms that are sorted in order of usage.
      /// </summary>
      /// <returns>string</returns>
      public string GetSearchTermList()
      {
         var dict = GetSearchTermDict(); // Assembles a list of search terms used across all documents and sorts them by frequency.
         var text = GetSearchTermIndex( dict ); // Save the list of search terms inside this Lucene index.
         return text;
      }

      /// <summary>
      /// Writes the content specified to a large field in Lucene. (Lucene does not support large fields so this method will split the data
      /// across multiple lucene documents.  GetLargeField() handles the reassembling of the data into a single string).
      /// </summary>
      public void SetLargeField( string field, string content )
      {
         // Lucene documents have a max char cap of a little over 30k bytes so we are staying within
         // the 1st 30k bytes to stay on the safe side.
         if ( content.Length > MAX_WORDS )
         {
            int idx = 0;
            // In the case of large documents we are going to split the content across several documents to get
            // around the character cap.  When the service is called on the other side the documents are
            // reassembled into a single document again.
            while ( content.Length != 0 )
            {
               string text = string.Empty;
               if ( content.Length > MAX_WORDS )
               {
                  // Break at the last line break that is within the 1st 25k characters.
                  for ( int i = MAX_WORDS; i > 0; i-- )
                  {
                     // Split our content at the line break.
                     if ( content.Substring( i, 1 ) == "\n" )
                     {
                        text = content.Substring( 0, i );
                        content = content.Substring( i );
                        break;
                     }
                  }
               }
               else
               {
                  text = content;
                  content = string.Empty;
               }

               // Write the ~30k character section to an indexed document. 
               var doc = new Document
               {
                  new StringField( "Id", field + idx, Field.Store.YES ),
                  new StringField( "Index", text, Field.Store.YES ),
               };

               // Add the document to the index.
               _writer.AddDocument( doc );
               idx++;
            }
         }
         else // If there are less than 30000 characters write to a single document.
         {
            // Create a new index document.
            var doc = new Document
            {
               new StringField( "Id", field, Field.Store.YES ),
               new StringField( "Index", content, Field.Store.YES ),
            };

            // Add it to our list.
            _writer.AddDocument( doc );
         }
      }

      /// <summary>
      /// Reads content from the specified field and returns the large data as a string. (Lucene does not support large data.  
      /// This method reads small data across multiple documents and reassembles it into a large string.  See LetLargeField().)
      /// </summary>
      /// <param name="field">The field to read from.</param>
      /// <returns>string</returns>
      public string GetLargeField( string field )
      {
         string output = string.Empty;

         // Build a list of PAs that we want to keep in the index.
         for ( int i = _reader.MaxDoc - 1; i > -1; i-- ) // Cycle through the index in reverse order.  We go in 
         {
            var doc = _reader.Document( i );
            var name = doc.Get ( "Id" );
            if ( name != null && name.StartsWith( field ) )
            {
               if ( name == field )
               {
                  output = doc.Get( field );
                  break;
               }
               else
               {
                  output = doc.Get( field ) + output;
                  if ( name == field + "0" )
                  {
                     break;
                  }
               }
            }
         }

         return output;
      }

      /// <summary>
      /// Adds a single document to the index.
      /// </summary>
      /// <param name="obj">The object to index.</param>
      /// <exception cref="InvalidDataException">We only support primitive types and DateTime.</exception>
      public void AddDocument( dynamic obj )
      {
         NameTypeAndValue[] namesAndTypes = GetNamesTypesAndValues( obj );
         Type               str           = typeof( string );
         Type               nbr           = typeof( int );
         Type               lng           = typeof( long );
         Type               dbl           = typeof( double );
         Type               dec           = typeof( decimal );
         Type               dtm           = typeof( DateTime );

         Document doc = new Document( );

         foreach ( var ntv in namesAndTypes )
         {

            if ( ntv.DataType == str )
            {
               if ( ntv.IsTextSearchable )
               {
                  var ft =  new FieldType
                  {
                     IsIndexed        = true,
                     IsTokenized      = true,
                     IsStored         = true,
                     StoreTermVectors = true
                  };
                  var value = ntv.Value == null ? string.Empty : ntv.Value;
                  Field tf = new Field( ntv.Name, value, ft );
                  doc.Fields.Add( tf );
               }
               else
               {
                  StringField sf = new StringField( ntv.Name, ntv.Value == null ?
                     string.Empty : ntv.Value, Field.Store.YES );
                  doc.Fields.Add( sf );
               }
            }
            else if ( ntv.DataType == nbr )
            {
               Int32Field nf = new Int32Field( ntv.Name, ntv.Value == null ? 0 : ntv.Value, Field.Store.YES );
               doc.Fields.Add( nf );
            }
            else if ( ntv.DataType == lng ||
                      ntv.DataType == dbl ||
                      ntv.DataType == dec )
            {
               DoubleField df = new DoubleField( ntv.Name, ntv.Value == null ? 0 : (double)ntv.Value, Field.Store.YES );
               doc.Fields.Add( df );
            }
            else if ( ntv.DataType == dtm )
            {
               DoubleField df = new DoubleField( ntv.Name, ntv.Value == null ?
                  DateTime.MinValue.Ticks : ((DateTime)ntv.Value).Ticks, Field.Store.YES );
               doc.Fields.Add( df );
            }
            else
            {
               throw new InvalidDataException( "Unsupported data type '" + ntv.DataType + "'" );
            }
         }

         _writer.AddDocument( doc );
      }

      /// <summary>
      /// Adds a list of documents to the index.
      /// </summary>
      /// <param name="documents">The list of documents to add.</param>
      /// <exception cref="InvalidDataException">This method can only intake type System.Collections.Generic.List.</exception>
      public void AddDocuments( dynamic documents )
      {
         string type = documents.GetType().ToString();

         if ( !type.StartsWith( "System.Collections.Generic.List" ) )
         {
            throw new InvalidDataException( "Add documents must receive a generic list of type System.Collections.Generic.List." );
         }

         foreach ( var document in documents )
         {
            AddDocument( document );
         }
      }

      /// <summary>
      /// Clears the entire contents of an index.  Included in memory, local temporary, and remote azure file systems.
      /// Cannot be undone.  Use with caution.
      /// </summary>
      /// <exception cref="AccessViolationException">This method is not available in read only mode.</exception>
      public void Clear()
      {
         _writer.DeleteAll();
         _writer.Commit();
      }

      /// <summary>
      /// Clears the entire contents of an index in local temporary, and remote azure file systems without instantiating
      /// the IndexWriter.  Cannot be undone.  Use with caution.
      /// </summary>
      /// <exception cref="AccessViolationException">This method is not available in read only mode.</exception>
      public static void Clear( IndexingConfiguration config )
      {
         DeleteRemoteIndex( config );
      }

      /// <summary>
      /// Validates that this code is running on a TextSearch server if so returns true otherwise returns false.  
      /// </summary>
      /// <returns>bool</returns>
      public static bool ValidateServer()
      {
         var fredIndexing = Environment.GetEnvironmentVariable( "FRED_INDEXING" );
         return fredIndexing != null && string.Equals( "ON", fredIndexing, StringComparison.OrdinalIgnoreCase );
      }

      /// <summary>
      /// Disposes of all dependencies.  This always needs to be called before exiting.  If it is not called it will 
      /// likely corrupt the index.
      /// </summary>
      public void Dispose()
      {
         if ( _writer != null )
         {
            try
            {
               TryCommit( CommitType.Flush );   // Write all pending changes to Lucene index.
               TryCommit( CommitType.Dispose ); // Dispose of writer and related resources.
            }
            catch ( Exception e )
            {
               _config.Logger.LogException( e );
            }
            finally
            {
               _writer = null; // Nulling object for good measure.
            }
         }

         if ( _folder != null )
         {
            try
            {
               _folder.ClearLock( "write.lock" ); // Make sure the write lock lease has been removed.
            }
            catch ( Exception e )
            {
               _config.Logger.LogException( e );
            }
            finally
            {
               _folder.Dispose();                  // Dispose of the folder once we are done with it.
               _folder = null;                     // Nulling object for good measure.
            }
         }

         if ( _reader != null )
         {
            _reader.Dispose();
            _reader = null;
         }

         // The locks on the TEMP files do not get released until garbage collector is called.
         // https://stackoverflow.com/questions/13262548/delete-a-file-being-used-by-another-process
         GC.Collect();
         GC.WaitForPendingFinalizers();

         // Once we are done with it delete the remote write.lock file.  Removed async in order to have ref objects for this method.
         DeleteBlobFileAsync( RemotePath + @"/write.lock" ).Wait();
      }

      /// <summary>
      /// Deletes the specified from from Azure Blob Storage. 
      /// </summary>
      private async Task<bool> DeleteBlobFileAsync( string fileName )
      {
         var cloudStorageAccount = CloudStorageAccount.Parse( "DefaultEndpointsProtocol=https;" +
                                                              "AccountName="                    +
                                                              _config.Blob.GetAccount()         +
                                                              ";AccountKey="                    +
                                                              _config.Blob.GetKey()             +
                                                              ";EndpointSuffix=core.windows.net" );
         CloudBlobClient    cloudBlobClient    = cloudStorageAccount.CreateCloudBlobClient();
         CloudBlobContainer cloudBlobContainer = cloudBlobClient.GetContainerReference( _config.Blob.GetContainer() );
         CloudBlob          cloudBlob          = cloudBlobContainer.GetBlobReference( fileName );

         // Lucene.Net.Storage.Azure uses Azure file locks instead of the old write.lock convention.  However the old
         // file is still being created.  This code deletes the file because it could be confusing to see a write.lock
         // in indexes that are not being written to.  If the write.lock file is present id doesn't break anything.
         try
         {
            await cloudBlob.DeleteIfExistsAsync();
            return true;
         }
         catch ( Exception e )
         {
            _config.Logger.LogWarning( "The " + fileName + " write.lock failed to delete. " + e.Message );
            return false;
         }
      }

      /// <summary>
      /// Get a dictionary of search terms for this index.
      /// </summary>
      /// <returns>KeyValuePair[string, long]</returns>
      private List<KeyValuePair<string, long>> GetSearchTermDict()
      {
         IDictionary<string, long> frequencyMap = new Dictionary<string, long>();

         // Loads a list of terms that we exclude from indexing.  These are usually extremely common words that add
         // no value in the type ahead in the interface.
         string       json                = File.ReadAllText( "ExcludedSearchTerms.json" );
         List<string> excludedSearchTerms = Newtonsoft.Json.JsonConvert.DeserializeObject<List<string>>( json );
         for ( int i = 0; i < _reader.MaxDoc; i++ )
         {
            Terms fileTerms, titleTerms;
            try
            {
               fileTerms = _reader.GetTermVectors( i ).GetTerms( "FileText" );  // Get search terms from file.
               titleTerms = _reader.GetTermVectors( i ).GetTerms( "TitleText" ); // Get search terms from title.
            }
            catch ( NullReferenceException )
            {
               continue;
            }

            AddToFrequencies( frequencyMap, fileTerms, excludedSearchTerms );  // Adds terms from file to the search term list.
            AddToFrequencies( frequencyMap, titleTerms, excludedSearchTerms ); // Adds terms from title to the search term list.
         }

         var sorted = frequencyMap.OrderByDescending( x => x.Value ).ToList();

         return sorted;
      }

      /// <summary>
      /// Records an assembled list of search terms into the index for this PA.
      /// </summary>
      private string GetSearchTermIndex( List<KeyValuePair<string, long>> frequencyMap )
      {
         StringBuilder sb = new StringBuilder();
         for ( int i = 0; i < frequencyMap.Count; i++ )
         {
            sb.Append( frequencyMap[i].Key + "\n" );
         }
         string list = sb.ToString().Trim();

         return list;
      }

      /// <summary>
      /// Performs search term generation within the scope specified by terms object.
      /// </summary>
      private void AddToFrequencies( IDictionary<string, long> frequencyMap, Terms terms, List<string> excludedSearchTerms )
      {
         if ( terms == null )
         {
            return;
         }

         TermsEnum enumerator = terms.GetEnumerator();

         next_term:
         while ( enumerator.MoveNext() )
         {
            BytesRef word     = enumerator.Term;
            string   freqTerm = word.Utf8ToString();

            #region Exclude numbers, <3 character terms, and excluded terms.

            if ( int.TryParse( freqTerm.Substring( 0, 1 ), out _ ) ||
                 freqTerm.Length < 3 ||
                 excludedSearchTerms.Contains( freqTerm ) )
            {
               continue;
            }

            #endregion

            #region Assemble list of common search terms.

            for ( int n = 0; n < frequencyMap.Count; n++ )
            {
               if ( frequencyMap.Keys.Contains( freqTerm ) )
               {
                  if ( frequencyMap[freqTerm] != enumerator.TotalTermFreq )
                  {
                     frequencyMap[freqTerm] += enumerator.TotalTermFreq; // combine occurrences across documents
                  }

                  goto next_term;
               }
            }

            frequencyMap.Add( new KeyValuePair<string, long>( freqTerm, enumerator.TotalTermFreq ) );

            #endregion
         }
      }

      /// <summary>
      /// Gets the path to the remote index in blob storage.
      /// </summary>
      /// <param name="config">The indexer configurations settings to use.</param>
      /// <returns>string</returns>
      private static string GetRemotePath( IndexingConfiguration config )
      {
         string path = "fredblobdocumentindexing/" + config.FilePath;
         return path;
      }

      /// <summary>
      /// Creates an IndexWriter object.  Includes important error handling needed for scaling. 
      /// </summary>
      private Task TryCreateIndexWriterAsync( LuceneDirectory lucDir )
      {
         var task = Task.Factory.StartNew( ( ) =>
         {
            bool        completed = false;
            DateTime    end       = DateTime.UtcNow.AddSeconds( 120 ); // Time out if lock is not released after 2 minutes.
            int         storage   = 0, alreadySet = 0, obtainFailed = 0;
            while ( DateTime.UtcNow < end )
            {
               try
               {
                  // Configuration for IndexWriter
                  var analyzer = new StandardAnalyzer( LuceneVersion.LUCENE_48 );
                  var conf     = new IndexWriterConfig( LuceneVersion.LUCENE_48, analyzer );
                  /*{
                     OpenMode = await GetNeedsCreatedAsync() ? OpenMode.CREATE : OpenMode.APPEND
                  };*/

                  _writer = new IndexWriter( lucDir, conf ); // Create writer

                  completed = true;                            // Exit completely
                  break;                                       // Exit now
               }
               //catch ( StorageException )
               //{
               //   Thread.Sleep( 200 ); // Wait for other thread to finish.
               //   storage++;
               //}
               catch ( AlreadySetException )
               {
                  Thread.Sleep( 200 ); // Wait for other thread to finish.
                  alreadySet++;
               }
               catch ( LockObtainFailedException )
               {
                  Thread.Sleep( 200 ); // Wait for lock to release.
                  obtainFailed++;
               }
            }

            if ( ! completed ) // Throw timeout exception if process exceeds 2 minutes.
            {
               throw new TimeoutException(
                  $"Write lock not released within 120 seconds while creating IndexWriter. storage {storage} already {alreadySet} obtain {obtainFailed} " );
            }
         } );

         return task;
      }

      /// <summary>
      /// Returns a Directory object from Azure to local.
      /// </summary>
      private LuceneDirectory GetFolder( string rpath )
      {
         return GetAzureDirectory( rpath );
      }

      /// <summary>
      /// Gets an AzureDirectory object based on the remote path to stream data from Azure.
      /// </summary>
      private AzureDirectory GetAzureDirectory( string rpath )
      {
         // Establish credentials to access the data.
         StorageCredentials sc = new StorageCredentials( _config.Blob.GetAccount(), _config.Blob.GetKey() );
         // Log into the cloud storage account using credentials.
         CloudStorageAccount csa = new CloudStorageAccount( sc, "core.windows.net", true );
         // Get the remote folder path
         AzureDirectory folder = new AzureDirectory( csa, _config.Blob.GetContainer() + "/" + rpath );

         return folder;
      }

      /// <summary>
      /// Handles flushing (writing) data to the index and disposing of the writer.  Includes error catching at scale. 
      /// </summary>
      private void TryCommit( CommitType type )
      {
         bool     committed = false;
         DateTime end       = DateTime.UtcNow.AddSeconds( 30 );

         while ( DateTime.UtcNow < end )
         {
            try
            {
               switch ( type )
               {
                  case CommitType.Dispose:
                     _writer.Dispose(); // Release resources.
                     committed = true;  // Exit completely
                     break;             // Exit now
                  default:
                     // Write changes to index files.
                     _writer.Flush( triggerMerge: true, applyAllDeletes: true );
                     committed = true; // Exit completely
                     break;            // Exit now
               }

               // ReSharper disable once ConditionIsAlwaysTrueOrFalse
               if ( committed )
               {
                  break;
               }
            }
            catch ( LockObtainFailedException )
            {
               Thread.Sleep( 100 ); // Wait for lock to release.
            }
         }

         if ( !committed ) // Throw timeout exception if process exceeds 30 seconds.
         {
            throw new LockObtainFailedException( "Write lock not released within 30 seconds." );
         }
      }

      /// <summary>
      /// Gets a list of names, types, and values for the given object.
      /// </summary>
      /// <param name="obj">The object to analyze</param>
      /// <returns>NameTypeAndValue[]</returns>
      private NameTypeAndValue[] GetNamesTypesAndValues( dynamic obj )
      {
         var output  = new List<NameTypeAndValue>();
         var objType = obj.GetType();

         // get all properties of MyClass type
         PropertyInfo[] propertyInfos = objType.GetProperties();

         // sort properties by name
         Array.Sort( propertyInfos,
            ( propertyInfo1, propertyInfo2 ) =>
               String.Compare( propertyInfo1.Name, propertyInfo2.Name, StringComparison.Ordinal ) );

         // write property names
         foreach ( PropertyInfo propertyInfo in propertyInfos )
         {
            Type             str       = typeof( string );
            IsTextSearchableAttribute attribute = ( IsTextSearchableAttribute )propertyInfo
               .GetCustomAttributes()
               .FirstOrDefault( o => o.GetType() == typeof(IsTextSearchableAttribute) );
            if ( propertyInfo.PropertyType == str )
            {
               if ( attribute != null )
               {
                  output.Add( new NameTypeAndValue( propertyInfo.Name, propertyInfo.PropertyType,
                     propertyInfo.GetValue( obj ), attribute.IsSearchable ) );
               }
               else
               {
                  output.Add( new NameTypeAndValue( propertyInfo.Name, propertyInfo.PropertyType,
                     propertyInfo.GetValue( obj ), false ) );
               }
            }
            else
            {
               output.Add( new NameTypeAndValue( propertyInfo.Name, propertyInfo.PropertyType,
                  propertyInfo.GetValue( obj ), false ) );
            }
         }

         return output.ToArray();
      }

      /// <summary>
      /// Creates an index reader object.  It will also dispose of obsolete readers if needed.
      /// </summary>
      /// <returns>IndexReader</returns>
      /// <exception cref="AccessViolationException">If the open mode is set to write only reader is not available.</exception>
      public Task PrepareReaderAsync()
      {
         var task = Task.Factory.StartNew( ( ) =>
         {
            if ( _reader != null )
            {
               _reader.Dispose();
               _reader = null;
            }

            _reader ??= _writer == null ? DirectoryReader.Open( _folder ) : _writer.GetReader( true );
         });

         return task;
      }

      /// <summary>
      /// Asynchronously creates an index writer object.
      /// </summary>
      /// <returns>Task</returns>
      public Task PrepareWriterAsync()
      {
         return TryCreateIndexWriterAsync( _folder );
      }

      /// <summary>
      /// Gets and object of the specified type based of a Lucene document.
      /// </summary>
      /// <param name="doc">The Lucene document to read from.</param>
      /// <param name="query">The query phrase to search with.</param>
      /// <param name="field">The field to search on.</param>
      /// <param name="score">The score of the search result.</param>
      /// <typeparam name="T">The type of object to create.</typeparam>
      /// <returns>T</returns>
      /// <exception cref="InvalidDataException">We only support simple types and DateTime.</exception>
      private T GetObjectOfType<T>( Document doc, Query query, string field, float score ) where T : new()
      {
         T output = new T();
         Type objType = typeof(T);

         // get all properties of MyClass type
         PropertyInfo[] propertyInfos = objType.GetProperties();

         // sort properties by name
         Array.Sort( propertyInfos,
            ( propertyInfo1, propertyInfo2 ) =>
               String.Compare( propertyInfo1.Name, propertyInfo2.Name, StringComparison.Ordinal ) );

         // write property names
         foreach ( PropertyInfo propertyInfo in propertyInfos )
         {
            //output.Add( propertyInfo.Name );
            if ( propertyInfo.PropertyType == typeof( string ) )
            {
               IsTextSearchContextAttribute attribute = ( IsTextSearchContextAttribute )propertyInfo
                  .GetCustomAttributes()
                  .FirstOrDefault( o => o.GetType() == typeof( IsTextSearchContextAttribute ) );

               if ( attribute is { IsContext: true } )
               {
                  propertyInfo.SetValue( output, GetContext( field, doc, query ) );
               }
               else
               {
                  var str = doc.Get( propertyInfo.Name );
                  propertyInfo.SetValue( output, str );
               }
            }
            else if ( propertyInfo.PropertyType == typeof( int ) )
            {
               var integer = int.Parse( doc.Get( propertyInfo.Name ) );
               propertyInfo.SetValue( output, integer );
            }
            else if ( propertyInfo.PropertyType == typeof( double ) )
            {
               var dbl = double.Parse( doc.Get( propertyInfo.Name ) );
               propertyInfo.SetValue( output, dbl );
            }
            else if ( propertyInfo.PropertyType == typeof( float ) )
            {
               IsTextSearchScoreAttribute attribute = ( IsTextSearchScoreAttribute )propertyInfo
                  .GetCustomAttributes()
                  .FirstOrDefault( o => o.GetType() == typeof(IsTextSearchScoreAttribute) );

               if ( attribute is { IsScore: true } )
               {
                  propertyInfo.SetValue( output, score );
               }
               else
               {
                  var flt = float.Parse( doc.Get( propertyInfo.Name ).Split( '.' )[0] );
                  propertyInfo.SetValue( output, flt );
               }
            }
            else if ( propertyInfo.PropertyType == typeof( long ) )
            {
               var lng = long.Parse( doc.Get( propertyInfo.Name ).Split( '.' )[0] );
               propertyInfo.SetValue( output, lng );
            }
            else if ( propertyInfo.PropertyType == typeof( decimal ) )
            {
               var dec = decimal.Parse( doc.Get( propertyInfo.Name ) );
               propertyInfo.SetValue( output, dec );
            }
            else if ( propertyInfo.PropertyType == typeof( DateTime ) )
            {
               var dttm = new DateTime( long.Parse( doc.Get( propertyInfo.Name ).Split( '.' )[0] ) );
               propertyInfo.SetValue( output, dttm );
            }
            else
            {
               throw new InvalidDataException( "Unsupported data type '" + propertyInfo.PropertyType + "'" );
            }
         }

         return output;
      }

      /// <summary>
      /// This method gets the context around the search results of a given search.
      /// </summary>
      /// <param name="field">The field to search on.</param>
      /// <param name="doc">The document to search.</param>
      /// <param name="query">The query to perform.</param>
      /// <returns>string</returns>
      private string GetContext( string field, Document doc, Query query )
      {

         #region Get scoring for each match found and prep for context creation.

         var formatter   = new SimpleHTMLFormatter( _config.OpenTag, _config.CloseTag ); // Create formatter with start and end tags.
         var scorer      = new QueryScorer( query );                                   // Get the match score.
         var highlighter = new Highlighter( formatter, scorer )
         {
            // Create context parser.
            TextFragmenter = new SimpleSpanFragmenter( scorer, _config.ContextLength )
         }; // Create context highlighter  

         #endregion

         // Get the document body text.  Remove line breaks and duplicate spaces for clean looking context.
         var bt = doc.Get( field );

         // Get the context and apply HTML markup.
         string context  = highlighter.GetBestFragment( _analyzer,  field, bt );

         return context;
      }

      /// <summary>
      /// This method deletes all files in the specified remote folder.  It will break any write locks encountered.
      /// </summary>
      /// <param name="config">The indexer configurations to use.</param>
      private static void DeleteRemoteIndex( IndexingConfiguration config )
      {
         var remote = GetRemotePath( config );
         var task   = config.Blob.ListAsync( remote ); // Get a list of files in that location.
         task.Wait();
         var blobDocs = task.Result;

         foreach ( IBlobItem doc in blobDocs )
         {
            // Delete each file found.
            BreakLease( config.Blob, doc.BlobName );
            config.Blob.DeleteAsync( doc.BlobName ).Wait();
         }
      }

      /// <summary>
      /// If a write lock lease is present on a file this method will attempt to break it.
      /// </summary>
      /// <param name="blob">The blob interface to use to perform blob operations.</param>
      /// <param name="path">The path of the file to break.</param>
      private static async Task BreakLease( IBlobStorage blob, string path )
      {
         if ( await blob.ExistsAsync( path ) )
         {
            try
            {
               // If file is present then make sure that the file is lease free so that it can be deleted.
               await blob.BreakLeaseAsync( path ); // Break any leases.
            }
            catch ( StorageException e )
            {
               if ( e.Message != "There is currently no lease on the blob." &&
                    e.Message != "The specified blob does not exist." ) // Let no lease exception pass through.
               {
                  throw;
               }
            }
         }
      }

      /// <summary>
      /// Simple struct used to store information about a property. 
      /// </summary>
      private readonly struct NameTypeAndValue
      {
         public string Name { get; }
         public Type DataType { get; }
         public dynamic Value { get; }
         public bool IsTextSearchable { get; }

         public NameTypeAndValue( string name, Type type, dynamic value, bool textSearchable )
         {
            Name = name;
            DataType = type;
            Value = value;
            IsTextSearchable = textSearchable;
         }
      }

      /// <summary>
      /// Determines whether TryCommit should write data to index of dispose of the indexer object.
      /// </summary>
      private enum CommitType
      {
         Flush,
         Dispose
      }
   }
}
