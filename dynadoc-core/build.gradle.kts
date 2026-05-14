plugins {
    id("org.jetbrains.kotlin.plugin.serialization") version "2.3.21"
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("aws.sdk.kotlin:dynamodb-jvm:[1.3.0, 2[")

    testImplementation(kotlin("test"))
    testImplementation(platform("org.junit:junit-bom:5.14.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.mockk:mockk:1.14.9")
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name = "Dynadoc"
                description = "Dynadoc is a Kotlin library for using DynamoDB as a JSON document store."
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
