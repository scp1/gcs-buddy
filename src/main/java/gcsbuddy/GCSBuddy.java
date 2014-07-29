package gcsbuddy;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.googleapis.media.MediaHttpDownloaderProgressListener;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.CharSource;
import org.apache.http.NoHttpResponseException;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static gcsbuddy.Logging.info;
import static gcsbuddy.Logging.warn;

@SuppressWarnings("UnusedDeclaration")
/**
 * Provides an easy-to-use wrapper around the Google Cloud Storage (GCS) JSON API client lib, with convenience methods
 * that simplify tasks like:
 *   - iterating over lists of GCS objects
 *   - reading bytes or characters directly from an existing GCS object
 *   - composing a new GCS object in GCS out of 2 or more source objects
 *   - retrying operations on an API error
 */
public final class GCSBuddy {

  private static final String PREFIX_DELIMITER = "/";

  // Controls how often upload/download progress is reported when using progress reporters
  private static final Long PROGRESS_INTERVAL_IN_BYTES = 1024L * 1024L;

  // Used for upload requests
  private static final String UPLOAD_CONTENT_TYPE = "application/octet-stream";
  private static final String ATTACHMENT = "attachment";

  // Controls the page size used when iterating over storage objects.
  private static final long DEFAULT_PAGE_SIZE = 200;

  private static final class IsRetryableGoogleException implements Predicate<Object> {
    @Override
    public boolean apply(@Nullable Object throwable) {
      checkNotNull(throwable);
      assert throwable != null;
      if (throwable instanceof GoogleJsonResponseException) {
        final GoogleJsonError details = ((GoogleJsonResponseException) throwable).getDetails();
        return details != null && details.getCode() >= 500;
      }
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  // operations will be retried according to the retry policy when any of the following are caught
  // during the execution of a request
  private static Predicate<Object> SHOULD_RETRY = Predicates.or(
    Lists.newArrayList(
      new IsRetryableGoogleException(),
      Predicates.instanceOf(SocketTimeoutException.class),
      Predicates.instanceOf(NoHttpResponseException.class)
    )
  );

  private static final class NAME_FN implements Function<StorageObject, String> {
    @Nullable
    @Override
    public String apply(@Nullable StorageObject object) {
      return checkNotNull(object, "object").getName();
    }
  }

  private static final class COMPOSE_SOURCE_OBJECT_FN implements Function<String, ComposeRequest.SourceObjects> {
    @Nullable
    @Override
    public ComposeRequest.SourceObjects apply(@Nullable String objectName) {
      return new ComposeRequest.SourceObjects().setName(objectName);
    }
  }

  private static final class REMOVE_LEADING_DELIMITER_FN implements Function<String, String> {
    @Nullable
    @Override
    public String apply(@Nullable String input) {
      return removeLeadingDelimiter(input);
    }
  }

  private static String removeLeadingDelimiter(String prefix) {
    // Object prefixes look just like file paths, except they have no leading '/' in GCS
    return Strings.nullToEmpty(prefix).startsWith(PREFIX_DELIMITER) ? prefix.substring(1) : prefix;
  }

  private final Supplier<Storage> storage;
  private final String bucketName;
  private final Supplier<RetryStrategy> retryStrategies;

  public GCSBuddy(final Storage storage,
                  final String bucketName) {
    this(storage, bucketName, RetryStrategies.noRetry());
  }

  public GCSBuddy(final Storage storage,
                  final String bucketName,
                  final RetryStrategy retryStrategy) {
    this.storage = Suppliers.ofInstance(checkNotNull(storage, "storage"));
    this.bucketName = bucketName;
    checkArgument(!Strings.isNullOrEmpty(this.bucketName), "bucketName");
    this.retryStrategies = Suppliers.ofInstance(checkNotNull(retryStrategy, "retryStrategy"));
  }

  @Inject
  GCSBuddy(final Supplier<Storage> storage,
           final @Named("gcs-buddy.bucket-name") String bucketName,
           final Supplier<RetryStrategy> retryStrategies) {
    this.storage = checkNotNull(storage, "storage");
    this.bucketName = bucketName;
    checkArgument(!Strings.isNullOrEmpty(this.bucketName), "bucketName");
    this.retryStrategies = checkNotNull(retryStrategies, "retryStrategies");
  }

  /**
   * Returns a new {@link java.util.Iterator} that pages over lists of objects with the supplied prefix
   * @param prefix the prefix to use
   * @throws IOException
   */
  private Iterator<StorageObject> newPagedObjectIterator(final String prefix) throws IOException {

    final Storage.Objects.List listRequest = storage.get()
      .objects()
      .list(bucketName)
      .setMaxResults(DEFAULT_PAGE_SIZE);

    if (!Strings.isNullOrEmpty(prefix)) {
      listRequest.setPrefix(prefix);
    }

    final RetryStrategy retryStrategy = retryStrategies.get();

    return new AbstractIterator<StorageObject>() {

      private Objects currentObjects = null;
      private Iterator<StorageObject> currentObjectsIter = null;
      private boolean firstPage = true;

      /**
       * Attempts to advance to the next page of results from the list command
       * @return true if a new page was advanced to and that new page has objects, false otherwise
       * @throws IOException
       */
      private boolean advancePage() throws IOException {

        currentObjects = executeWithRetry(listRequest);

        if (currentObjects == null || currentObjects.getItems() == null) {
          return false;
        }

        currentObjectsIter = currentObjects.getItems().iterator();
        return currentObjectsIter.hasNext();
      }

      @Override
      protected StorageObject computeNext() {

        try {
          if (firstPage) {
            if (!advancePage()) {
              return endOfData();
            }
            firstPage = false;
          }

          if (currentObjectsIter.hasNext()) {
            return currentObjectsIter.next();
          } else {
            // current page is empty, get the next one
            String nextPageToken = currentObjects.getNextPageToken();
            if (nextPageToken == null) {
              return endOfData();
            }
            listRequest.setPageToken(nextPageToken);

            return advancePage() ? currentObjectsIter.next() : endOfData();
          }
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }

    };
  }

  private Iterable<StorageObject> newPagedObjectIterable(final String prefix) {
    return new Iterable<StorageObject>() {
      @Override
      public Iterator<StorageObject> iterator() {
        try {
          return newPagedObjectIterator(prefix);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private <T> T executeWithRetry(final StorageRequest<T> request) throws IOException {

    final RetryStrategy retryStrategy = retryStrategies.get();

    try {
      while(retryStrategy.shouldRetry()) {
        try {
          return request.execute();
        } catch (Throwable e) {
          if (!maybeSleep(e, retryStrategy, request)) {
            throw e;
          }
        }
      }
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }

    throw new RetriesExhaustedException(
      String.format("retries exhausted for request: %s using strategy: %s", request, retryStrategy)
    );
  }

  private InputStream executeAsInputStreamWithRetry(final StorageRequest<?> request) throws IOException {
    final RetryStrategy retryStrategy = retryStrategies.get();

    try {
      while(retryStrategy.shouldRetry()) {
        try {
          return request.executeAsInputStream();
        } catch (Throwable e) {
          if (!maybeSleep(e, retryStrategy, request)) {
            throw e;
          }
        }
      }
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }

    throw new RetriesExhaustedException(
      String.format("retries exhausted for request: %s using strategy: %s", request, retryStrategy)
    );
  }

  /**
   * Blocks for some interval specified by {@code retryStrategy} iff the request should be retried (according to the
   * type of exception thrown) and returns false otherwise.
   * @param throwable the exception that was thrown while attempting to execute the request
   * @param retryStrategy the retry strategy in use
   * @param request the request being executed
   * @return true iff the request is to be retried and the sleep interval has elapsed. {@code} false if the request
   * should not be retried
   * @throws InterruptedException
   */
  private boolean maybeSleep(final Throwable throwable,
                             final RetryStrategy retryStrategy,
                             final StorageRequest<?> request) throws InterruptedException {
    if (!SHOULD_RETRY.apply(throwable)) {
      return false;
    } else {
      final long interval =  retryStrategy.nextRetryIntervalInMs();
      warn(
        "caught {0} while executing {1}; retrying in {2}ms...",
        throwable.getClass().getName(),
        request.getClass().getName(),
        interval
      );
      warn(throwable);
      Thread.sleep(interval);
      return true;
    }
  }

  /**
   * Returns the name of the bucket that {@code this} is configured to use
   */
  public String bucketName() {
    return bucketName;
  }

  /**
   * Lists object names in GCS that have the given prefix.
   * @param prefix the prefix to use
   * @return GCS object names that match the supplied prefix, if any.
   */
  public Iterable<String> ls(final String prefix) {
    if (prefix == null) {
      return Lists.newArrayList();
    }

    return Iterables.transform(newPagedObjectIterable(removeLeadingDelimiter(prefix)), new NAME_FN());
  }

  /**
   * Lists objects in GCS that have the given prefix.
   * @param prefix the prefix to use
   * @return GCS {@link com.google.api.services.storage.model.StorageObject} instances that match the supplied prefix, if any.
   */
  public Iterable<StorageObject> lsObjects(final String prefix) {
    if (prefix == null) {
      return Lists.newArrayList();
    }

    return newPagedObjectIterable(removeLeadingDelimiter(prefix));
  }

  /**
   * Uploads the data contained in the supplied {@code byteSource} to the specified object name.
   * @param objectName the name to use for newly-uploade object
   * @param byteSource the source of the bytes that comprise the object
   */
  public void upload(final String objectName, final ByteSource byteSource) throws IOException {
    upload(objectName, byteSource, false);
  }

  /**
   * Uploads the data contained in the supplied {@code byteSource} to the specified object name.
   * @param objectName the path to upload to
   * @param byteSource the source of the bytes to write
   * @param reportProgress true to report periodic upload progress to stdout.  NOTE!! The progress reporter calls
   *                       .size() on the provided {@code byteSource} in order to determine the overall size.  Do not
   *                       use the progress reporting unless you know that that underlying {@code byteSource} can be
   *                       manipulated in such a way. For example, this would cause problems if the {@code byteSource}
   *                       were a stream, and the call to .size() would cause an unwanted seek.
   */
  public void upload(final String objectName, final ByteSource byteSource, final boolean reportProgress)
    throws IOException {
    checkNotNull(byteSource, "byteSource");
    checkArgument(!Strings.isNullOrEmpty(removeLeadingDelimiter(objectName)), "objectName cannot be empty");

    final String cleanObjName = removeLeadingDelimiter(objectName);

    InputStreamContent content = new InputStreamContent(UPLOAD_CONTENT_TYPE, byteSource.openBufferedStream());

    // For small files, you may wish to call setDirectUploadEnabled(true), to
    // reduce the number of HTTP requests made to the server.
    // theStorageObject.getMediaHttpUploader().setDirectUploadEnabled(true);

    Storage.Objects.Insert insert = storage.get().objects()
      .insert(bucketName,
              new StorageObject()
                .setName(cleanObjName)
                .setContentDisposition(ATTACHMENT),
              content);

    if (reportProgress) {
      insert.getMediaHttpUploader()
        .setProgressListener(
          UploadProgressListener.create(cleanObjName, PROGRESS_INTERVAL_IN_BYTES, byteSource.size())
        );
    }

    executeWithRetry(insert);
  }

  /**
   * Downloads the data from an object and writes the bytes to the specified {@link com.google.common.io.ByteSink}
   * @param objectName the object to download
   * @param byteSink the destination to write the bytes to
   * @throws IOException
   */
  public void download(final String objectName, final ByteSink byteSink) throws IOException {
    download(objectName, byteSink, false);
  }

  /**
   * Downloads the data from an object and writes the bytes to the specified {@link com.google.common.io.ByteSink}
   * @param objectName the object to download
   * @param byteSink the destination to write the bytes to
   * @param reportProgress true to report periodic download progress to stdout.
   * @throws IOException
   */
  public void download(final String objectName, final ByteSink byteSink, final boolean reportProgress)
    throws IOException {
    checkNotNull(byteSink, "byteSink");
    checkArgument(!Strings.isNullOrEmpty(removeLeadingDelimiter(objectName)), "object name cannot be empty");

    final String cleanObjName = removeLeadingDelimiter(objectName);

    // For small files, you may wish to call setDirectDownloadEnabled(true), to
    // reduce the number of HTTP requests made to the server.
    // theStorageObject.getMediaHttpDownloader().setDirectDownloadEnabled(true);

    Storage.Objects.Get get = storage.get().objects()
      .get(bucketName, cleanObjName);

    if (reportProgress) {
      StorageObject objMetadata = get.execute();
      get.getMediaHttpDownloader()
        .setProgressListener(
          DownloadProgressListener.create(cleanObjName, PROGRESS_INTERVAL_IN_BYTES, objMetadata.getSize().longValue())
        );
    }

    get.executeMediaAndDownloadTo(byteSink.openBufferedStream());
  }

  /**
   * Returns a new {@link com.google.common.io.ByteSource} for reading bytes from the given object.
   * @param objectName the name of the object from which to read bytes
   * @return
   * @throws IOException
   */
  public ByteSource readBinary(final String objectName) throws IOException {
    checkArgument(!Strings.isNullOrEmpty(removeLeadingDelimiter(objectName)), "path cannot be empty");

    final String cleanObjName = removeLeadingDelimiter(objectName);

    final Storage.Objects.Get get = storage.get().objects()
      .get(bucketName, cleanObjName);

    final InputStream inputStream = executeAsInputStreamWithRetry(get);

    return new ByteSource() {
      @Override
      public InputStream openStream() throws IOException {
        return inputStream;
      }
    };
  }

  /**
   * Returns a new {@link com.google.common.io.CharSource} for reading characters from the given object.
   * @param objectName the name of the object from which to read characters
   * @param charset the character set to use when reading from the object
   * @throws IOException
   */
  public CharSource read(final String objectName, final Charset charset) throws IOException {
    checkNotNull(charset, "charset");
    return readBinary(objectName).asCharSource(charset);
  }

  /**
   * Composes multiple objects into a single destination object.  This behavior (combined with an upload) may be used to
   * simulate appending to a GCS object.
   * @param destinationObjectName The name of the newly-composed object
   * @param sourceObjectNames The names of the objects to compose into the destination object
   * @return the componentCount of the newly-composed object.  GCS does not allow objects to have a componentCount
   * > 1024
   * @throws IOException
   */
  public int compose(final String destinationObjectName, final Collection<String> sourceObjectNames) throws IOException {
    checkArgument(!Strings.isNullOrEmpty(destinationObjectName), "destinationPath cannot be empty");
    checkArgument(!Iterables.isEmpty(sourceObjectNames), "sourcePaths cannot be empty");

    final String cleanDestinationObjName = removeLeadingDelimiter(destinationObjectName);

    ImmutableList<ComposeRequest.SourceObjects> sourceObjects = FluentIterable
      .from(sourceObjectNames)
      .transform(new REMOVE_LEADING_DELIMITER_FN())
      .transform(new COMPOSE_SOURCE_OBJECT_FN())
      .toList();

    StorageObject destinationObj = new StorageObject()
      .setBucket(bucketName)
      .setName(cleanDestinationObjName)
      .setContentType(UPLOAD_CONTENT_TYPE);

    Storage.Objects.Compose compose = storage.get()
      .objects()
      .compose(
        bucketName,
        cleanDestinationObjName,
        new ComposeRequest().setSourceObjects(sourceObjects).setDestination(destinationObj)
      );

    StorageObject response = executeWithRetry(compose);
    return response.getComponentCount();
  }

  /**
   * Deletes the specified object
   * @param objectName
   * @throws IOException
   */
  public void rm(final String objectName) throws IOException {
    storage.get()
      .objects()
      .delete(bucketName, objectName)
      .execute();
  }

  private static abstract class ProgressListener {

    protected final Stopwatch stopwatch = Stopwatch.createUnstarted();

    protected final long intervalInBytes;
    protected final String objectName;
    protected final String operation;
    protected final Optional<Long> totalSizeInBytes;
    protected long bytesTransferred = 0L;


    ProgressListener(String objectName, String operation, long intervalInBytes, Optional<Long> totalSizeInBytes) {
      checkArgument(!Strings.isNullOrEmpty(objectName), "objectName cannot be empty");
      checkArgument(intervalInBytes > 0, "intervalInBytes must be > 0");
      checkArgument(!totalSizeInBytes.isPresent() || totalSizeInBytes.get() > 0, "totalSizeInBytes must be > 0");

      this.objectName = objectName;
      this.operation = Strings.nullToEmpty(operation);
      this.intervalInBytes = intervalInBytes;
      this.totalSizeInBytes = totalSizeInBytes;
    }

    void started() {
      stopwatch.start();
      info("{0} of {1} started...", operation, objectName);
    }

    void progress(long numBytesTransferred) {
      long newBytesTransferred = numBytesTransferred;
      if ((newBytesTransferred - bytesTransferred) >= intervalInBytes) {
        if (totalSizeInBytes.isPresent()) {
          info(
            "{0} : ({1}%) {2}ed {3}/{4} bytes...",
            objectName,
            100 * newBytesTransferred / totalSizeInBytes.get(),
            operation,
            newBytesTransferred,
            totalSizeInBytes.get()
          );
        } else {
          info("{0} : {1}ed {2} bytes...", objectName, operation, newBytesTransferred);
        }
      }
      bytesTransferred = newBytesTransferred;
    }

    void complete() {
      stopwatch.stop();
      info("{0} of {1} completed in {2}", operation, objectName, stopwatch);
    }
  }

  private static class DownloadProgressListener extends ProgressListener implements MediaHttpDownloaderProgressListener {

    static DownloadProgressListener create(String objectName, long intervalInBytes) {
      return new DownloadProgressListener(objectName, intervalInBytes, Optional.<Long>absent());
    }

    static DownloadProgressListener create(String objectName, long intervalInBytes, long totalSizeInBytes) {
      checkArgument(!Strings.isNullOrEmpty(objectName), "objectName cannot be empty");
      checkArgument(intervalInBytes > 0, "intervalInBytes must be > 0");
      checkArgument(totalSizeInBytes > 0, "totalSizeInBytes must be > 0");

      return new DownloadProgressListener(objectName, intervalInBytes, Optional.of(totalSizeInBytes));
    }

    private DownloadProgressListener(String objectName, long intervalInBytes, Optional<Long> totalSizeInBytes) {
      super(objectName, "download", intervalInBytes, totalSizeInBytes);
    }

    @Override
    public void progressChanged(MediaHttpDownloader downloader) {
      switch (downloader.getDownloadState()) {
        case MEDIA_IN_PROGRESS:
          double progress = downloader.getProgress();
          progress(downloader.getNumBytesDownloaded());
          break;
        case MEDIA_COMPLETE:
          complete();
          break;
        default:
          break;
      }
    }
  }

  private static class UploadProgressListener extends ProgressListener implements MediaHttpUploaderProgressListener {

    static UploadProgressListener create(String objectName, long intervalInBytes) {
      return new UploadProgressListener(objectName, intervalInBytes, Optional.<Long>absent());
    }

    static UploadProgressListener create(String objectName, long intervalInBytes, long totalSizeInBytes) {
      checkArgument(!Strings.isNullOrEmpty(objectName), "objectName cannot be empty");
      checkArgument(intervalInBytes > 0, "intervalInBytes must be > 0");
      checkArgument(totalSizeInBytes > 0, "totalSizeInBytes must be > 0");

      return new UploadProgressListener(objectName, intervalInBytes, Optional.of(totalSizeInBytes));
    }

    private UploadProgressListener(String objectName, long intervalInBytes, Optional<Long> totalSizeInBytes) {
      super(objectName, "upload", intervalInBytes, totalSizeInBytes);
    }

    @Override
    public void progressChanged(MediaHttpUploader uploader) {
      switch (uploader.getUploadState()) {
        case INITIATION_STARTED:
          started();
          break;
        case MEDIA_IN_PROGRESS:
          progress(uploader.getNumBytesUploaded());
          break;
        case MEDIA_COMPLETE:
          complete();
          break;
        default:
          break;
      }
    }
  }

}
