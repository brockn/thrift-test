package com.cloudera.test.thrift;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import com.cloudera.test.thrift.generated.CrawlingService;

public class Server {
  
  private void start(int port) throws Exception {
    TServerSocket serverTransport = new TServerSocket(port);
    CrawlingHandler handler = new CrawlingHandler();
    CrawlingService.Processor<CrawlingService.Iface> processor
            = new CrawlingService.Processor<CrawlingService.Iface>(handler);

    TServer server = new TThreadPoolServer(
            new TThreadPoolServer.Args(serverTransport).processor(processor));

    System.out.println("Starting server on port " + port);
    server.serve();
  }

  public static void main(String args[]) throws Exception {
    try {
      Server server = new Server();
      server.start(Integer.parseInt(args[0]));
    } catch(Error error) {
      error.printStackTrace();
    }
  }
}
