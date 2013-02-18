package com.continuuity.gateway.auth;

import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Authenticator used when authentication is disabled.
 */
public class NoAuthenticator implements GatewayAuthenticator {

  @Override
  public boolean authenticateRequest(HttpRequest request) {
    return true;
  }

  @Override
  public boolean isAuthenticationRequired() {
    return false;
  }

}
