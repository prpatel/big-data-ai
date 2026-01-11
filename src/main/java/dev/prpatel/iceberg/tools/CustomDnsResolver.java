package dev.prpatel.iceberg.tools;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class CustomDnsResolver implements InetAddressResolver {

    // Our hardcoded, in-memory "hosts" file.
    private static final Map<String, InetAddress> HOSTS;
    private final InetAddressResolver defaultResolver;

    // Initialize custom host mappings
    static {
        Map<String, InetAddress> hosts = new HashMap<>();
        try {
            hosts.put("minio", InetAddress.getByAddress("localhost", new byte[]{127, 0, 0, 1}));
            hosts.put("localhost", InetAddress.getByAddress("localhost", new byte[]{127, 0, 0, 1}));
            // examples if more are needed to override other docker services or other places
//            hosts.put("api.internal", InetAddress.getByAddress("api.internal", new byte[]{(byte)192, (byte)168, 1, 100}));
//            hosts.put("db.prod.internal", InetAddress.getByAddress("db.prod.internal", new byte[]{10, 10, 20, 55}));
        } catch (UnknownHostException e) {
            // This error is fatal for static initialization
            throw new Error("Failed to initialize custom host mappings", e);
        }
        HOSTS = Collections.unmodifiableMap(hosts);
    }

    // Constructor receives the built-in resolver for fallback
    public CustomDnsResolver(InetAddressResolver defaultResolver) {
        this.defaultResolver = defaultResolver;
    }

    @Override
    public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy) throws UnknownHostException {
        InetAddress customAddress = HOSTS.get(host);

        // If the host is in our custom map, return its IP address
        if (customAddress != null) {
            System.out.println(">>> [CustomResolver] Resolving '" + host + "' to '" + customAddress.getHostAddress() + "'");
            return Stream.of(customAddress);
        }

        // Otherwise, delegate to the default system resolver
        System.out.println(">>> [CustomResolver] Host '" + host + "' not found. Delegating to default resolver...");
        return defaultResolver.lookupByName(host, lookupPolicy);
    }

    @Override
    public String lookupByAddress(byte[] addr) throws UnknownHostException {
        // For reverse lookups, we just delegate to the default resolver
        return defaultResolver.lookupByAddress(addr);
    }
}