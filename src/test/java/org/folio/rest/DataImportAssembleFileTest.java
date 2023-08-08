package org.folio.rest;


import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;

import org.apache.http.HttpStatus;

import org.folio.rest.jaxrs.model.AssembleFileDto;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.ProcessSplitFilesRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.s3.client.S3ClientFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.localstack.LocalStackContainer;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;

import io.restassured.RestAssured;
import io.restassured.mapper.ObjectMapperType;
import io.restassured.path.json.JsonPath;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class DataImportAssembleFileTest extends AbstractRestTest {

  private static final String ASSEMBLE_PATH = "/data-import/assembleStorageFile";
  private static final String UPLOAD_URL_PATH = "/data-import/uploadUrl";
  private static final String UPLOAD_URL_CONTINUE_PATH = "/data-import/uploadUrl/subsequent";
  private static final String START_SPLIT_PATH = "/data-import/uploadDefinitions/processSplitFiles";
  @Test
  public void shouldAssembleFile(TestContext context) {
    //start upload
  
    JsonPath info1 =  RestAssured.given()
        .spec(spec)
        .when()
        .queryParam("fileName", "test-name1")
        .get(UPLOAD_URL_PATH ).jsonPath();
    String uploadId1 = info1.get("uploadId");
    String key1 = info1.get("key");
   
    
    String url1 = info1.get("url");
    ArrayList<String> tags = putFakeFile(context, url1, 5 * 1024 * 1024);

    //upload 2nd piece
    JsonPath info2 = RestAssured
    .given()
    .spec(spec)
    .when()
    .queryParam("key", key1)
    .queryParam("uploadId",uploadId1)
    .queryParam("partNumber", "2")
    .get(UPLOAD_URL_CONTINUE_PATH)
    .then()
    .statusCode(HttpStatus.SC_OK).log().all()
    .extract().body().jsonPath();

    String url2 = info2.get("url");
 
    tags.addAll( putFakeFile(context, url2,5 * 1024 * 1024));

    AssembleFileDto dto =  new AssembleFileDto();
    dto.setUploadId(uploadId1);
    dto.setKey(key1);
    dto.setTags(tags);
    RestAssured.given()
      .spec(spec)
      .body(dto, ObjectMapperType.GSON)
      .when()
      .post(ASSEMBLE_PATH )
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    
  }
  @Test
  public void shouldFailAssembleFileFailedPartUpload(TestContext context) { 
    
    JsonPath info1 =  RestAssured.given()
        .spec(spec)
        .when()
        .queryParam("fileName", "test-name1")
        .get(UPLOAD_URL_PATH ).jsonPath();
    String uploadId1 = info1.get("uploadId");
    String key1 = info1.get("key");
   
    
    String url1 = info1.get("url");
    ArrayList<String> tags = putFakeFile(context, url1, 1 * 1024 * 1024);

    //upload 2nd piece
    JsonPath info2 = RestAssured
    .given()
    .spec(spec)
    .when()
    .queryParam("key", key1)
    .queryParam("uploadId",uploadId1)
    .queryParam("partNumber", "2")
    .get(UPLOAD_URL_CONTINUE_PATH)
    .then()
    .statusCode(HttpStatus.SC_OK).log().all()
    .extract().body().jsonPath();

    String url2 = info2.get("url");
 
    tags.addAll( putFakeFile(context, url2,5 * 1024 * 1024));
    

    AssembleFileDto dto =  new AssembleFileDto();
    dto.setUploadId(uploadId1);
    dto.setKey(key1);
    dto.setTags(tags);
    RestAssured.given()
      .spec(spec)
      .body(dto, ObjectMapperType.GSON)
      .when()
      .post(ASSEMBLE_PATH )
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }
  @Test
  public void shouldStartSplitProcess(TestContext context) {
    
    UploadDefinition uploadDef = new UploadDefinition().withId(UUID.randomUUID().toString());
    JobExecution jobExecution = new JobExecution()
        .withId("5105b55a-b9a3-4f76-9402-a5243ea63c97")
        .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
        .withSubordinationType(JobExecution.SubordinationType.PARENT_MULTIPLE)
        .withStatus(JobExecution.Status.NEW)
        .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
        .withUserId(UUID.randomUUID().toString());

      WireMock.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions"), true))  
        .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(jobExecution).encode())));
      WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/data-import/uploaddefinition/" + uploadDef.getId()), true))  
          .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(uploadDef).encode())));
    
      ProcessSplitFilesRqDto newDto = new ProcessSplitFilesRqDto().withUploadDefinition(uploadDef);
      RestAssured.given()
        .spec(spec)
        .when()
        .body(newDto)
        .post(START_SPLIT_PATH )
        .then()
        .log().all()
        .statusCode(HttpStatus.SC_NO_CONTENT);
    
    

  }
  private ArrayList<String> putFakeFile(TestContext context, String url1, int size) {
    ArrayList<String> tags = new ArrayList<String>();
    try {
      URL urlobj = new URL(url1);
      HttpURLConnection con = (HttpURLConnection) urlobj.openConnection();
      con.setRequestMethod("PUT");
      con.setDoOutput(true);
      OutputStream output = con.getOutputStream();
      output.write(new byte[size]);
      tags.add(con.getHeaderField("eTag"));
    } catch (Exception e) {
      context.fail(e.getMessage());
    }
    return tags;
  }
}
