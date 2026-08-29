#!/bin/bash -eu
# ClusterFuzzLite build script (OSS-Fuzz conventions, language: jvm).
#
# Builds the library, compiles the fuzz targets from .clusterfuzzlite/fuzz/
# against it, and writes one Jazzer wrapper per target into $OUT. The regular
# build insists on Java 21; the fuzz build deliberately targets 17
# (-Djava.version=17, the library compiles cleanly against it) because the
# OSS-Fuzz base images build AND run on a JDK 17 - class files above 61
# would not load in the runner.

cd "$SRC/tabellarium"

mvn --batch-mode --no-transfer-progress -DskipTests -Djacoco.skip=true -Djava.version=17 \
  package dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/fuzz-deps

cp target/tabellarium-*.jar "$OUT/tabellarium.jar"
cp target/fuzz-deps/*.jar "$OUT/"

ALL_JARS=$(cd "$OUT" && echo *.jar)
# Compile against absolute jar paths plus the Jazzer API; at runtime every
# jar and the target classes sit next to the wrapper ($this_dir).
BUILD_CLASSPATH=$(echo "$ALL_JARS" | xargs printf -- "$OUT/%s:")$JAZZER_API_PATH
RUNTIME_CLASSPATH=$(echo "$ALL_JARS" | xargs printf -- '$this_dir/%s:')'$this_dir'

for fuzzer in "$SRC/tabellarium/.clusterfuzzlite/fuzz/"*.java; do
  fuzzer_basename=$(basename -s .java "$fuzzer")
  javac --release 17 -cp "$BUILD_CLASSPATH" -d "$OUT" "$fuzzer"

  # Wrapper per OSS-Fuzz JVM convention. The LLVMFuzzerTestOneInput marker
  # below is how the infrastructure recognizes the file as a fuzz target.
  cat > "$OUT/$fuzzer_basename" <<EOF
#!/bin/bash
# LLVMFuzzerTestOneInput - magic string for fuzz-target detection.
this_dir=\$(dirname "\$0")
if [[ "\$@" =~ (^| )-runs=[0-9]+(\$| ) ]]; then
  mem_settings='-Xmx1900m:-Xss900k'
else
  mem_settings='-Xmx2048m:-Xss1024k'
fi
LD_LIBRARY_PATH="\$JVM_LD_LIBRARY_PATH":\$this_dir \
\$this_dir/jazzer_driver --agent_path=\$this_dir/jazzer_agent_deploy.jar \
--cp=$RUNTIME_CLASSPATH \
--target_class=$fuzzer_basename \
--jvm_args="\$mem_settings" \
\$@
EOF
  chmod +x "$OUT/$fuzzer_basename"
done
