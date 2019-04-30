package org.ldg.proxy

import java.net.InetSocketAddress

import org.scalatest.FlatSpec

class DiscoverProxyTest extends FlatSpec {

  "discoverProxies" should "discover proxies" in {
    val fixture = new DiscoverProxies
    val proxies = fixture.discoverProxies()
    for(proxy <- proxies) yield {
      println("proxy hostname : " + proxy.`type`())
      val addr = proxy.address().asInstanceOf[InetSocketAddress]

      if(addr == null) {
        println("No Proxy")
      } else {
        println(s"proxy hostname : ${addr.getHostName}")
        println(s"proxy port : ${addr.getPort}")
      }
    }
  }
}
