odps-lot-proto 使用 protobuf 2.4.1 编译 common, log 包下的 proto 文件。

自 0.52.0 版本开始，不再每次编译的过程中重新编译，而是使用预编译好的文件。

如果需要重新编译，可以在 pom 文件中增加如下插件。
```bash
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-antrun-plugin</artifactId>
                <version>1.8</version>
                <executions>
                    <execution>
                        <id>compile-protoc</id>
                        <phase>generate-sources</phase>
                        <configuration>
                            <tasks>
                                <mkdir dir="target/generated-sources"/>
                                <path id="proto.path">
                                    <fileset dir="./common/">
                                        <include name="**/*.proto"/>
                                    </fileset>
                                    <fileset dir="./lot/">
                                        <include name="**/*.proto"/>
                                    </fileset>
                                </path>
                                <pathconvert pathsep=" " property="proto.files"
                                             refid="proto.path"/>
                                <exec executable="./protoc/protoc-2.4.1.sh" failonerror="true"
                                      osfamily="unix">
                                    <arg value="--java_out=${basedir}/target/generated-sources"/>
                                    <arg value="-I${basedir}"/>
                                    <arg line="${proto.files}"/>
                                </exec>
                                <exec executable="./protoc/protoc-2.4.1.sh" failonerror="true"
                                      osfamily="mac">
                                    <arg value="--java_out=${basedir}/target/generated-sources"/>
                                    <arg value="-I${basedir}"/>
                                    <arg line="${proto.files}"/>
                                </exec>
                                <exec executable="${basedir}/protoc/protoc.exe"
                                      failonerror="true" osfamily="windows">
                                    <arg value="--java_out=${basedir}/target/generated-sources"/>
                                    <arg value="-I${basedir}"/>
                                    <arg line="${proto.files}"/>
                                </exec>

                            </tasks>
                            <sourceRoot>target/generated-sources</sourceRoot>
                        </configuration>
                        <goals>
                            <goal>run</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
```