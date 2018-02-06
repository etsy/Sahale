package com.etsy.sahale

import org.apache.commons.httpclient.{HttpClient, HttpStatus}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.http.apache.ApacheHttpTransport
import org.apache.commons.httpclient.methods.{GetMethod, PostMethod}
import org.apache.commons.httpclient.NameValuePair
import com.google.api.client.googleapis.auth.oauth2.{GoogleIdTokenVerifier, GoogleCredential, GooglePublicKeysManager}
import com.google.api.client.json.webtoken.{JsonWebToken, JsonWebSignature}
import java.util.concurrent.atomic.AtomicBoolean
import java.net.URI
import cascading.flow.Flow
import scala.util.{Try, Success}
import java.io.FileInputStream
import java.util.Collections
import java.net.URLEncoder

case class IdToken(audience: String, transport: HttpClient, serviceAccountJsonFile: Option[String] = None) {
  private var _token: Option[String] = None
  private var _expiresAtSeconds: Option[Long] = None

  private def updateToken {
    val token = serviceAccountJsonFile.map { file =>
        IdToken.getTokenFromServiceAccountFlow(audience, transport, file)
    }.getOrElse(IdToken.getTokenFromMetadata(audience, transport))

    // the expiry checker also validates the token and raises an exception if
    // the token is invalid.  So compute both before storing either.
    val expiry = IdToken.getExpiresAtSeconds(token, audience)

    _token = Some(token)
    _expiresAtSeconds = Some(expiry)
  }

  def isExpired: Boolean = {
    // Returns true if the token has not yet been retrieved, or if the token
    // has expired
    _expiresAtSeconds.forall { expSeconds =>
      System.currentTimeMillis / 1000 <= expSeconds
    }
  }

  def token: String = {
    if(isExpired) {
      updateToken
    }

    // updateToken raises an exception if it fails, so if we're here then we
    // know that _token is populated
    _token.get
  }
}

object IdToken {
  val IDENTITY_TOKEN_METADATA_URI = "http://metadata/computeMetadata/v1/instance/service-accounts/default/identity"
  val GOOGLE_TOKEN_ENDPOINT = "https://www.googleapis.com/oauth2/v4/token"

  def getTokenFromMetadata(audience: String, transport: HttpClient): String = {
    // see: https://cloud.google.com/compute/docs/instances/verifying-instance-identity
    val request = new GetMethod(IDENTITY_TOKEN_METADATA_URI)
    request.addRequestHeader("Metadata-Flavor", "Google")
    request.setQueryString(Array(new NameValuePair("audience", audience)))

    var token: Option[String] = None

    try {
      val code = transport.executeMethod(request)

      if(code != HttpStatus.SC_OK) {
        //LOG.warn(s"Metadata server returned failure code on identity-token request: $code")
      }

      token = Some(new String(request.getResponseBody, "UTF-8"))
    } catch { case e: Throwable =>
        //LOG.warn(s"Failed to refresh identity token from metadata server: $e")
    } finally {
      request.releaseConnection
    }

    token.getOrElse {
      sys.error("Failed to refresh identity token")
    }
  }

  def getTokenFromServiceAccountFlow(audience: String, transport: HttpClient, filename: String): String ={
    // This is a 2-step flow to get a Google-signed ID token starting from a
    // service account.
    // see: https://cloud.google.com/endpoints/docs/openapi/service-account-authentication#using_a_google_id_token

    // Step 1: Construct a token that we self-sign using the private key for
    //         the service account.  We set the audience to the google token
    //         API's URL, and we set the target_audience assertion to the
    //         audience for which we want the Google ID token

    val credentials = getServiceAccountCredentials(filename)
    val selfSignedToken = getServiceAccountSignedToken(audience, credentials)

    // Step 2: Send the self-signed token to the Google token endpoint, and it
    //         will send us back a Google-signed ID token that we can send to
    //         the upstream service
    exchangeToken(selfSignedToken, transport)
  }

  val keyManager = new GooglePublicKeysManager(new ApacheHttpTransport, new JacksonFactory)
  def getExpiresAtSeconds(token: String, audience: String): Long = {
    val verifier = new GoogleIdTokenVerifier.Builder(keyManager)
      .setAudience(Collections.singletonList(audience))
      .setIssuer("https://accounts.google.com")
      .build

    val parsedToken = Option(verifier.verify(token)).getOrElse {
      sys.error("Failed to parse id token!")
    }

    parsedToken.getPayload.getExpirationTimeSeconds
  }

  private def getServiceAccountSignedToken(audience: String, creds: GoogleCredential): String = {
    val header = new JsonWebSignature.Header
    header.setType("JWT")
    header.setAlgorithm("RS256")
    //header.setKeyId(creds.getServiceAccountPrivateKeyId)

    val nowSeconds = System.currentTimeMillis / 1000
    val payload = new JsonWebToken.Payload
    payload.setIssuedAtTimeSeconds(nowSeconds)
    payload.setExpirationTimeSeconds(nowSeconds + 3600)
    payload.setIssuer(creds.getServiceAccountId)
    payload.set("target_audience", audience)
    payload.setAudience(GOOGLE_TOKEN_ENDPOINT)

    JsonWebSignature.signUsingRsaSha256(
      creds.getServiceAccountPrivateKey,
      new JacksonFactory,
      header,
      payload)
  }

  private def getServiceAccountCredentials(filename: String): GoogleCredential = {
    val stream = new FileInputStream(filename)
    val creds = GoogleCredential.fromStream(stream)
    stream.close

    creds
  }

  private def exchangeToken(token: String, transport: HttpClient): String = {
    // see: https://cloud.google.com/compute/docs/instances/verifying-instance-identity
    val uri = GOOGLE_TOKEN_ENDPOINT

    val request = new PostMethod(uri)

    val params = Array(
      new NameValuePair("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
      new NameValuePair("assertion", token)
    )

    request.setRequestBody(params)

    var idToken: Option[String] = None

    try {
      val code = transport.executeMethod(request)

      if(code != HttpStatus.SC_OK) {
        sys.error(s"Token endpoint failed with code $code: ${request.getResponseBody}")
      }

      val idTokenJson = new String(request.getResponseBody, "UTF-8")

      val parser = (new JacksonFactory).createJsonParser(idTokenJson)
      parser.skipToKey("id_token")

      idToken = Some(parser.getText)
    } catch { case e: Throwable =>
        //LOG.warn(s"Failed to refresh identity token from metadata server: $e")
    } finally {
      request.releaseConnection
    }

    idToken.getOrElse {
      sys.error("Failed to retrieve google-signed identity token")
    }
  }
}

class GoogleAuthFlowTracker(
  flow: Flow[_],
  runCompleted: AtomicBoolean,
  hostPort: String,
  disableProgressBar: Boolean,
  serviceAccountJsonFilename: String) extends FlowTracker(flow, runCompleted, hostPort, disableProgressBar) {

  // More java-compatibility constructors
  def this(flow: Flow[_], runCompleted: AtomicBoolean, hostPort: String, disableProgressBar: java.lang.Boolean) = {
    this(flow, runCompleted, hostPort, disableProgressBar, null)
  }

  def this(flow: Flow[_], runCompleted: AtomicBoolean, hostPort: String) = {
    this(flow, runCompleted, hostPort, false, null)
  }

  def this(flow: Flow[_], runCompleted: AtomicBoolean) = {
    this(flow, runCompleted, "", false, null)
  }

  // Refuse to run if the server host is not using HTTPS
  Try(new URI(this.serverHostPort).getScheme) match {
    case Success("https") => // OK
    case _ =>
      sys.error(s"Invalid host ${this.serverHostPort}: Google Auth is only valid over https!")
  }

  @transient // should not generally happen, but do not allow credentials to be serialized
  private val idToken: IdToken = IdToken(
    audience = this.serverHostPort,
    transport = FlowTracker.getHttpClient,
    serviceAccountJsonFile = Option(serviceAccountJsonFilename))

  override def setAdditionalHeaders = Map( "Authorization" -> s"Bearer ${idToken.token}" )
}
