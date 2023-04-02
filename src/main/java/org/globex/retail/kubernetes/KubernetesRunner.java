package org.globex.retail.kubernetes;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class KubernetesRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesRunner.class);

    @Inject
    KubernetesClient client;

    @SuppressWarnings("unchecked")
    public int run() {

        String connectionSecretName = System.getenv().getOrDefault("KAFKA_CONNECTION_SECRET", "kafka-client-secret");
        if (connectionSecretName == null || connectionSecretName.isBlank()) {
            LOGGER.error("Environment variable 'KAFKA_CONNECTION_SECRET' for kafka connection secret not set. Exiting...");
            return -1;
        }

        String kafkaConnectCustomResourceName = System.getenv().getOrDefault("KAFKA_CONNECT_NAME", "kafka-connect");
        if (kafkaConnectCustomResourceName == null || kafkaConnectCustomResourceName.isBlank()) {
            LOGGER.error("Environment variable 'KAFKA_CONNECT_NAME' for kafka connect name not set. Exiting...");
            return -1;
        }
        String namespace = System.getenv("NAMESPACE");
        if (namespace == null || namespace.isBlank()) {
            LOGGER.error("Environment variable 'NAMESPACE' for namespace not set. Exiting...");
            return -1;
        }

        // read kafka connection secret
        Secret connectionSecret = client.secrets().inNamespace(namespace).withName(connectionSecretName).get();
        if (connectionSecret == null) {
            LOGGER.error("Secret " + connectionSecretName + " not found in namespace " + namespace);
            return -1;
        }

        String bootstrapServerKey = System.getenv().getOrDefault("KEY_KAFKA_BOOTSTRAP_SERVER", "bootstrapServer");
        String bootstrapServerBase64 = connectionSecret.getData().get(bootstrapServerKey);
        if (bootstrapServerBase64 == null || bootstrapServerBase64.isBlank()) {
            LOGGER.error("Secret " + connectionSecretName + " does not contain a key for " + bootstrapServerKey + ". Exiting...");
            return -1;
        }
        String bootstrapServer = new String(Base64.getDecoder().decode(bootstrapServerBase64));

        String clientIdKey = System.getenv().getOrDefault("KEY_CLIENT_ID", "clientId");
        String clientIdBase64 = connectionSecret.getData().get(clientIdKey);
        if (clientIdBase64 == null || clientIdBase64.isBlank()) {
            LOGGER.error("Secret " + connectionSecretName + " does not contain a key for " + clientIdKey + ". Exiting...");
            return -1;
        }
        String clientId = new String(Base64.getDecoder().decode(clientIdBase64));

        String securityProtocolKey = System.getenv().getOrDefault("KEY_SECURITY_PROTOCOL", "securityProtocol");
        String securityProtocolBase64 = connectionSecret.getData().get(securityProtocolKey);
        if (securityProtocolBase64 == null || securityProtocolBase64.isBlank()) {
            LOGGER.error("Secret " + connectionSecretName + " does not contain a key for " + securityProtocolKey + ". Exiting...");
            return -1;
        }
        String securityProtocol = new String(Base64.getDecoder().decode(securityProtocolBase64));

        String saslMechanismKey = System.getenv().getOrDefault("KEY_CLIENT_ID", "saslMechanism");
        String saslMechanismBase64 = connectionSecret.getData().get(saslMechanismKey);
        if (saslMechanismBase64 == null || saslMechanismBase64.isBlank()) {
            LOGGER.error("Secret " + connectionSecretName + " does not contain a key for " + saslMechanismKey + ". Exiting...");
            return -1;
        }
        String saslMechanism = new String(Base64.getDecoder().decode(saslMechanismBase64));

        //KafkaConnect cr
        ResourceDefinitionContext context = new ResourceDefinitionContext.Builder()
                .withGroup("kafka.strimzi.io")
                .withVersion("v1beta2")
                .withKind("KafkaConnect")
                .withPlural("kafkaconnects")
                .withNamespaced(true)
                .build();

        GenericKubernetesResource kafkaConnect = client.genericKubernetesResources(context)
                .inNamespace(namespace)
                .withName(kafkaConnectCustomResourceName)
                .get();
        if (kafkaConnect == null) {
            LOGGER.error("KafkaConnect " + kafkaConnectCustomResourceName + " not found in namespace " + namespace + ". Exiting...");
            return -1;
        }

        Map<String, Object> additionalProperties = kafkaConnect.getAdditionalProperties();
        Map<String, Object> spec = (Map<String, Object>) additionalProperties.get("spec");

        // bootstrapServers
        spec.put("bootstrapServers", bootstrapServer);

        // authentication username
        Map<String, Object> authentication = (Map<String, Object>) spec.get("authentication");
        authentication.put("username", clientId);

        // authentication type
        if ("SCRAM_SHA_512".equals(saslMechanism)) {
            authentication.put("type", "scram-sha-512");
        }

        // security protocol
        if ("SASL_SSL".equals(securityProtocol)) {
            Map<String, Object> tls = new HashMap<>();
            String[] certs = {};
            tls.put("trustedCertificates", certs);
            spec.put("tls", tls);
        }

        //replicas
        spec.put("replicas", 1);

        client.genericKubernetesResources(context).inNamespace(namespace).resource(kafkaConnect).replace();

        return 0;

    }
}
