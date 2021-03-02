package com.esplugins.plugin.rescorer;

import com.codahale.metrics.MetricRegistry;
import com.esplugins.plugin.rescorer.utils.SecurityUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import java.net.Proxy;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HostnameVerifier;
import okhttp3.Authenticator;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.search.rescore.Rescorer;

public class DiscoveryClient   {
//
//  private Settings settings;
//  private static ServiceEndpointProvider serviceEndpointProvider;
//  private static OkHttpClient client;
//
//  @Inject
//  public DiscoveryClient(Settings settings) throws Exception{
//    this.settings = settings;
//    HttpClientConfiguration httpClientConfiguration = HttpClientConfiguration.builder()
//        .connections(10)
//        .host("samsara_v2.traefik.stg.phonepe.nb6")
//        .environment("prod")
//        .idleTimeOutSeconds(10)
//        .opTimeoutMs(200)
//        .secure(false)
//        .port(80)
//        .build();
//    try {
//       serviceEndpointProvider = new StaticServiceEndpointProvider(
//          httpClientConfiguration);
//       client = createClient("es", null, httpClientConfiguration,null,null,null,null);
//    }catch (Exception e){
//      e.printStackTrace();
//    }
//    System.out.println("in setting");
//  }
//
//  @Override
//  protected  void doStart(){
//
//    System.out.println("In Start");
//  }
//
//  @Override
//  protected  void doStop(){
//    System.out.println("In Stop");
//  }
//
//  @Override
//  protected  void doClose(){
//    System.out.println("In close");
//  }
//
//  private static OkHttpClient createClient(String clientName, MetricRegistry registry, HttpClientConfiguration configuration, HostnameVerifier hostnameVerifier, Proxy proxy, Authenticator proxyAuthenticator, SSLFactory sslFactory) throws Exception {
////    YggdrasilSetup
////        .process(configuration.isUsingZookeeper(), configuration.getServiceName(), configuration.getHost());
//    int connections = configuration.getConnections();
//    connections = connections == 0 ? 10 : connections;
//    int idleTimeOutSeconds = configuration.getIdleTimeOutSeconds();
//    idleTimeOutSeconds = idleTimeOutSeconds == 0 ? 30 : idleTimeOutSeconds;
//    int connTimeout = configuration.getConnectTimeoutMs();
//    connTimeout = connTimeout == 0 ? 10000 : connTimeout;
//    int opTimeout = configuration.getOpTimeoutMs();
//    opTimeout = opTimeout == 0 ? 10000 : opTimeout;
//    Dispatcher dispatcher = new Dispatcher();
//    dispatcher.setMaxRequests(connections);
//    dispatcher.setMaxRequestsPerHost(connections);
//    Builder clientBuilder = (new Builder()).connectionPool(new ConnectionPool(connections, (long)idleTimeOutSeconds, TimeUnit.SECONDS)).connectTimeout((long)connTimeout, TimeUnit.MILLISECONDS).readTimeout((long)opTimeout, TimeUnit.MILLISECONDS).writeTimeout((long)opTimeout, TimeUnit.MILLISECONDS).dispatcher(dispatcher);
//    if (proxy != null) {
//      clientBuilder.proxy(proxy);
//    }
//
//    if (proxyAuthenticator != null) {
//      clientBuilder.proxyAuthenticator(proxyAuthenticator);
//    }
//
//    if (hostnameVerifier != null) {
//      clientBuilder.hostnameVerifier(hostnameVerifier);
//    }
//
//    if (sslFactory != null) {
//      clientBuilder.sslSocketFactory(sslFactory.getSslSocketFactory(), sslFactory.getX509TrustManager());
//    }
//
//    return  SecurityUtils.doPrivilegedException(()-> clientBuilder.build());
//  }
//
//  public static  Map<String, Map<String,Float>> getScore(List<String> ids){
//      Endpoint endpoint = serviceEndpointProvider.endpoint().get();
//      try {
////        HystrixConfig hystrixConfig = new HystrixConfig();
////        HystrixConfigurationFactory.init(hystrixConfig);
//
////        List<String> appUniqueIds = new ArrayList<>();
////        appUniqueIds.add("tuytu");
////        appUniqueIds.add("87878");
//        Map<String,Object> map = new HashMap<>();
//        map.put("ids",ids);
//        ObjectMapper objectMapper = new ObjectMapper();
//        RequestBody requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"),
//            objectMapper.writeValueAsString(map));
//        System.out.println(client != null);
////        Response response = CommandFactory.<Response>create(
////            DiscoveryClient.class.getSimpleName(), "getApp").executor(() -> {
//
//          HttpUrl url = endpoint.url("/v1/housekeeping/score");
//
//        Response response =  SecurityUtils.doPrivilegedException(()->client.newCall(new Request.Builder()
//              .url(url)
//              .post(requestBody)
//              .build()).execute());
//
//       // }).execute();
//
//        if (!response.isSuccessful()) {
//          System.out.println(response.code());
//        }else {
//          String body = OkHttpUtils.bodyString(response);
//          TypeReference<HashMap<String, Map<String,Float>>> typeRef
//              = new TypeReference<HashMap<String, Map<String,Float>>>() {};
//          Map<String, Map<String,Float>> scores =  SecurityUtils.doPrivilegedException(()->objectMapper.readValue(body,typeRef));
//          return scores;
//        }
//
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
//      return null;
//    }

}
