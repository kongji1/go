function FindProxyForURL(url, host) {
    if (shExpMatch(host, "*.conn-service-cn-05.allawntech.com") || shExpMatch(host, "*.conn-service-cn-04.allawntech.com") || shExpMatch(host, "*.cloud.cupronickel.goog") || shExpMatch(host, "*.scone-pa.googleapis.com") || shExpMatch(host, "*.quake-pa.googleapis.com") || shExpMatch(host, "*.subscriptionsmanagement-pa.googleapis.com") || shExpMatch(host, "*.subscriptionsmobile-pa.googleapis.com") || shExpMatch(host, "*.federatedml-pa.googleapis.com") || shExpMatch(host, "*.na.b.g-tun.com") || shExpMatch(host, "*.userlocation.googleapis.com") || shExpMatch(host, "*.connectivitycheck.gstatic.com") || shExpMatch(host, "*.subscriptions") || shExpMatch(host, "*.clientservices.googleapis") || shExpMatch(host, "*.firebaseremoteconfig.googleapis.com") || shExpMatch(host, "*.d.adguard-dns.com")) {
        return "PROXY 127.0.0.1:7890";
    } else {
        return "DIRECT";
    }
}
