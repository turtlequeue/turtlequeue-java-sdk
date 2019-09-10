package turtlequeue.java.sdk;

import org.junit.Test;
import static org.junit.Assert.*;
import java.util.Map;
import java.util.Collections;
import java.util.HashMap;

public class TurtleQueueTest {
  @Test public void testIsAwesomeTurtleQueueMethod() {

    Map<String, String> m = new HashMap<String, String>();
    m.put("host", "turtlequeue.com");
    m.put("protocol", "https");
    m.put("type", "ws");
    TurtleQueue classUnderTest = new TurtleQueue(Collections.unmodifiableMap(m));
    assertTrue("isAwesome should return 'true'", classUnderTest.isAwesome());
  }
}
