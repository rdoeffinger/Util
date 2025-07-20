JUNIT=/usr/share/java/junit.jar
test -r "$JUNIT" || JUNIT=/usr/share/junit/lib/junit.jar
test -r "$JUNIT" || JUNIT=$(pwd)/junit-4.13.2.jar
javac -g src/com/hughes/util/*.java src/com/hughes/util/raf/*.java test/com/hughes/util/*.java test/com/hughes/util/raf/*.java -classpath "$JUNIT"
java -classpath "src:test:$JUNIT" junit.textui.TestRunner com.hughes.util.StringUtilTest
java -classpath "src:test:$JUNIT" junit.textui.TestRunner com.hughes.util.raf.UniformRAFListTest
java -classpath "src:test:$JUNIT" junit.textui.TestRunner com.hughes.util.raf.RAFListTest
