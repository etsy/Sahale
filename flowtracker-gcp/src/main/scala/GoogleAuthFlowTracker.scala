package com.etsy.sahale

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import java.util.concurrent.atomic.AtomicBoolean
import java.net.URI
import cascading.flow.Flow
import scala.util.{Try, Success}
import java.io.FileInputStream

object GoogleAuthFlowTracker {
  // When should we refresh our credentials?
  val CREDENTIALS_REFRESH_AGE_SECONDS = 100

  def refreshCredentialToken(credentials: GoogleCredential) {
    if(!credentials.refreshToken) {
      FlowTracker.LOG.warn("Could not refresh Google Auth token!")
    }
  }

  def getCredentials(serviceAccountJsonFilename: Option[String]): GoogleCredential = {
    serviceAccountJsonFilename.map { filename =>
        val stream = new FileInputStream(filename)
        val credential = GoogleCredential.fromStream(stream)
        stream.close
        credential
      }.getOrElse(GoogleCredential.getApplicationDefault)
  }
}

class GoogleAuthFlowTracker(
  flow: Flow[_],
  runCompleted: AtomicBoolean,
  hostPort: String,
  disableProgressBar: Boolean,
  serviceAccountJsonFilename: Option[String] = None) extends FlowTracker(flow, runCompleted, hostPort, disableProgressBar) {

  // Refuse to run if the server host is not using HTTPS
  Try(new URI(this.serverHostPort).getScheme) match {
    case Success("https") => // OK
    case _ =>
      sys.error(s"Invalid host ${this.serverHostPort}: Google Auth is only valid over https!")
  }

  @transient // should not generally happen, but do not allow credentials to be serialized
  private lazy val credentials = GoogleAuthFlowTracker.getCredentials(serviceAccountJsonFilename)

  private def getToken: String = {
    // credentials.getExpiresInSeconds() returns a java boxed Long, and sets
    // the value to null if called before the credentials have been refreshed.
    // Scala tries by default to cast this to a scala.Long, which fails.
    // So tell scala not to coerce the type
    val secondsToExpiry: Option[java.lang.Long] = Option(credentials.getExpiresInSeconds)
    if(secondsToExpiry.forall(seconds => seconds < GoogleAuthFlowTracker.CREDENTIALS_REFRESH_AGE_SECONDS)) {
      // Refresh the token if the expiry time is known to be less than the refresh age,
      // or if there is no expiry time defined at all (in which case, the token has not
      // yet been created).
      GoogleAuthFlowTracker.refreshCredentialToken(credentials)
    }

    credentials.getAccessToken
  }

  override def setAdditionalHeaders = Map( "Authorization" -> s"Bearer ${getToken}" )
}
