package com.cloudera.test.thrift;
import java.util.List;

import org.apache.thrift.TException;

import com.cloudera.test.thrift.generated.*;

public class CrawlingHandler implements CrawlingService.Iface {
  @Override
  public void write(List<Item> items) throws TException {
    for (Item item : items) {
      System.out.println(item);
    }
  }  
}
