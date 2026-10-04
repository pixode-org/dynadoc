dependencies {
    api(project(":dynadoc"))
    api("io.r2dbc:r2dbc-spi:1.0.0.RELEASE")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:1.10.2")

    testImplementation(kotlin("test"))
    testImplementation(platform("org.junit:junit-bom:5.14.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:testcontainers-tidb:2.0.5")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    testImplementation("io.asyncer:r2dbc-mysql:1.4.3")

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // Used by the TiDB container to check that the database is ready
    testRuntimeOnly("com.mysql:mysql-connector-j:26.7.0")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name = "Dynadoc TiDB"
                description = "A Kotlin library for storing typed objects JSON documents in a database-agnostic way, with optimistic concurrency and atomic multi-document updates."
                url = "https://github.com/pixode-org/dynadoc"
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
                developers {
                    developer {
                        name = "Flavien Charlon"
                        email = "flavien@charlon.org"
                    }
                }
                scm {
                    connection = "scm:git:git://github.com/pixode-org/dynadoc.git"
                    url = "https://github.com/pixode-org/dynadoc/tree/main"
                }
            }
        }
    }

    repositories {
        maven {
            url = uri(layout.buildDirectory.dir("../../publish/${project.name}-${project.version}"))
        }
    }
}

signing {
    sign(publishing.publications["maven"])
}
