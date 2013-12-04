package com.cloudera.test.thrift;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.cloudera.test.thrift.generated.CrawlingService;
import com.cloudera.test.thrift.generated.Item;

public class Client {
  public void write(final List<Item> items, String ... servers) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(servers.length);
    TTransport[] transports = new TTransport[servers.length];
    TProtocol[] protocols = new TProtocol[servers.length];
    CrawlingService.Client[] clients = new CrawlingService.Client[servers.length];
    try {
      for (int i = 0; i < servers.length; i++) {
        try {
          transports[i] = create(servers[i]);
          transports[i].open();
          protocols[i] = new TBinaryProtocol(transports[i]);
          clients[i] = new CrawlingService.Client(protocols[i]);
        } catch (Exception e) {
          throw new Exception("Error creating client for port " + servers[i], e);
        }
      }
      for (int i = 0; i < servers.length; i++) {
        final CrawlingService.Client client = clients[i];
        executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              for(Item item : items) {
                client.write(Collections.singletonList(item));
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        });
      }
      executor.shutdown();
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } finally {
      for (int i = 0; i < transports.length; i++) {
        if(transports[i] != null) {
          transports[i].close();
        }
      }
    }
  }
  private TSocket create(String server) {
    String[] parts = server.split(":");
    return new TSocket(parts[0], Integer.parseInt(parts[1].trim()));
  }
  public static void main(String[] args) throws Exception {
   List<Item> items = new ArrayList<Item>();
   for (int i = 0; i < 10000; i++) {
     items.add(new Item(i, String.valueOf(i)));
   }
   Client client = new Client();
   client.write(Collections.unmodifiableList(items), args);
  }
}