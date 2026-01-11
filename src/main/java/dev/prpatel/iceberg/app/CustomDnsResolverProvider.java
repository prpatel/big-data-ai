package dev.prpatel.iceberg.app;

import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;

public class CustomDnsResolverProvider extends InetAddressResolverProvider {

    @Override
    public InetAddressResolver get(Configuration configuration) {
        // Pass the built-in resolver to our custom implementation
        return new CustomDnsResolver(configuration.builtinResolver());
    }

    @Override
    public String name() {
        return "Custom DNS Resolver Provider";
    }
}