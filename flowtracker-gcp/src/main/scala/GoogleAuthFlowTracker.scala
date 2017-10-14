package com.etsy.sahale

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import java.util.concurrent.atomic.AtomicBoolean
import java.net.URI
import cascading.flow.Flow
import scala.util.{Try, Success}

object GoogleAuthFlowTracker {
  // When should we refresh our credentials?
  val CREDENTIALS_REFRESH_AGE_SECONDS = 100
}

class GoogleAuthFlowTracker(
  flow: Flow[_],
  runCompleted: AtomicBoolean,
  hostPort: String,
  disableProgressBar: Boolean) extends FlowTracker(flow, runCompleted, hostPort, disableProgressBar) {

  Try(new URI(this.serverHostPort).getScheme) match {
    case Success("https") =>
    case _ =>
      sys.error(s"Invalid host ${this.serverHostPort}: Google Auth is only valid over https!")
  }

  @transient
  private var credentials: GoogleCredential = null
  def getCredentials = Option(credentials).getOrElse {
    credentials = GoogleCredential.getApplicationDefault
    refreshCredentials // Must pre-refresh or token will be null, on GCE at least
    credentials
  }

  private def refreshCredentials {
    if(!getCredentials.refreshToken) {
      FlowTracker.LOG.warn("Could not refresh Google Auth token!")
    }
  }

  private def getToken: String = {
    // getCredentials.getExpiresInSeconds() returns a java boxed Long, and sets
    // the value to null when running on GCE.  Scala tries by default to cast
    // this to a scala.Long, which fails.  So tell scala not to coerce the type
    val secondsToExpiry: Option[java.lang.Long] = Option(getCredentials.getExpiresInSeconds)
    if(secondsToExpiry.exists(seconds => seconds < GoogleAuthFlowTracker.CREDENTIALS_REFRESH_AGE_SECONDS)) {
      refreshCredentials
    }

    credentials.getAccessToken
  }

  override def setAdditionalHeaders = Map( "Authorization" -> s"Bearer ${getToken}" )
}
