import java.nio.file.*;
import java.util.*;
public class NativeTrafficSummary {
  static class Group {
    long ops, nanos; int windows;
    Map<String,List<Long>> latency = new TreeMap<>();
  }
  public static void main(String[] args) throws Exception {
    Path root=Path.of(args[0]); var groups=new TreeMap<String,Group>();
    for(String line:Files.readAllLines(root.resolve("windows.csv")).subList(1,13)) {
      var f=line.split(","); int replicas=Integer.parseInt(f[1]);
      var g=groups.computeIfAbsent(f[1]+","+f[2],k->new Group());
      g.windows++;g.ops+=Long.parseLong(f[4]);g.nanos+=Long.parseLong(f[5]);
      for(int i=0;i<replicas;i++) for(String row:Files.readAllLines(root.resolve(f[0]+"-"+i+"-operations.csv"))) {
        var x=row.split(","); if(x[0].equals("measure")) g.latency.computeIfAbsent(x[3],k->new ArrayList<>()).add(Long.parseLong(x[5]));
      }
    }
    System.out.println("replicas,pool_per_replica,windows,operations,inclusive_ops_per_second,operation,samples,p50_ms,p95_ms");
    for(var e:groups.entrySet()) for(var l:e.getValue().latency.entrySet()) {
      var g=e.getValue(); var v=l.getValue(); Collections.sort(v);
      System.out.printf(Locale.ROOT,"%s,%d,%d,%.3f,%s,%d,%.3f,%.3f%n",e.getKey(),g.windows,g.ops,g.ops*1e9/g.nanos,l.getKey(),v.size(),v.get((int)Math.ceil(v.size()*.5)-1)/1e6,v.get((int)Math.ceil(v.size()*.95)-1)/1e6);
    }
  }
}
