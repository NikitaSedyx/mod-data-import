package org.folio.service.s3processing;

import io.vertx.core.Future;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public interface MarcRawSplitterService {

  Future<Integer> countRecordsInFile(InputStream inStream);


}
