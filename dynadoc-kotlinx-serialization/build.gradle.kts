plugins {
    id("org.jetbrains.kotlin.plugin.serialization") version "2.3.21"
}

dependencies {
    api(project(":dynadoc-core"))

    api("org.jetbrains.kotlinx:kotlinx-serialization-json:[1.6.0,1.11[")

    testImplementation(kotlin("test"))

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.mockk:mockk:1.13.11")
    testImplementation("org.skyscreamer:jsonassert:1.5.1")

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name = "Dynadoc kotlinx-serialization"
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
