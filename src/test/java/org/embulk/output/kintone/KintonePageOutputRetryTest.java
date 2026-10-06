package org.embulk.output.kintone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kintone.client.exception.KintoneApiRuntimeException;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.junit.Before;
import org.junit.Test;

public class KintonePageOutputRetryTest extends TestKintoneOutputPlugin {
  private static final ConfigMapper CONFIG_MAPPER =
      ConfigMapperFactory.builder().addDefaultModules().build().createConfigMapper();

  private ConfigSource config;

  @Before
  public void before() {
    config = loadConfigYaml("client/config.yml");
  }

  @Test
  public void testRetryableApiErrorNeedsKnownJsonCode() {
    assertTrue(
        KintonePageOutput.isRetryableApiError(
            KintoneClientTest.htmlErrorResponse(520, "{\"code\":\"GAIA_RE18\"}")));
    assertFalse(
        KintonePageOutput.isRetryableApiError(
            KintoneClientTest.htmlErrorResponse(400, "{\"code\":\"CB_VA01\"}")));
    assertFalse(
        KintonePageOutput.isRetryableApiError(
            KintoneClientTest.htmlErrorResponse(500, "{\"message\":\"no code\"}")));
    assertFalse(
        KintonePageOutput.isRetryableApiError(
            KintoneClientTest.htmlErrorResponse(400, KintoneClientTest.NO_CERT_HTML)));
    assertFalse(KintonePageOutput.isRetryableApiError(new RuntimeException("not an API error")));
  }

  @Test
  public void testUnwrapRetryFailureSurfacesConfigException() {
    ConfigException cause = new ConfigException("Failed to load client certificate");
    assertThat(
        KintonePageOutput.unwrapRetryFailure(new RetryGiveupException(cause), task()),
        is(sameInstance(cause)));
  }

  @Test
  public void testUnwrapRetryFailureExplainsNoCertResponse() {
    RuntimeException e =
        KintonePageOutput.unwrapRetryFailure(
            new RetryGiveupException(
                KintoneClientTest.htmlErrorResponse(400, KintoneClientTest.NO_CERT_HTML)),
            task());
    assertThat(e, is(instanceOf(ConfigException.class)));
    assertThat(
        e.getMessage(),
        is(
            "kintone at https://client rejected the request with HTTP 400 \"No Cert\". This domain requires client_certificate_path and client_certificate_password."));
  }

  @Test
  public void testUnwrapRetryFailureKeepsOtherErrorsWrapped() {
    KintoneApiRuntimeException cause =
        KintoneClientTest.htmlErrorResponse(400, "{\"code\":\"CB_VA01\"}");
    RetryGiveupException giveup = new RetryGiveupException(cause);
    RuntimeException e = KintonePageOutput.unwrapRetryFailure(giveup, task());
    assertThat(e.getMessage(), is("kintone throw exception"));
    assertThat(e.getCause(), is(sameInstance(giveup)));
  }

  private PluginTask task() {
    return CONFIG_MAPPER.map(config, PluginTask.class);
  }
}
