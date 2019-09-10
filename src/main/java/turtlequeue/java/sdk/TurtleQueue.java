package turtlequeue.java.sdk;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import java.util.Date;
import java.util.Map;

// see https://github.com/puredanger/clojure-from-java

public class TurtleQueue {


  static {
    IFn require = Clojure.var("clojure.core", "require");
    require.invoke(Clojure.read("turtlequeue.core"));
  }

  //private
  private static final IFn makeImpl = Clojure.var("turtlequeue.create", "make");

  private Object driver = null;

  public boolean isAwesome() {
    return true;
  }

  public TurtleQueue(Map<String, String> m) {
    driver = makeImpl.invoke(m);
  }

}
