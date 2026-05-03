import org.jetbrains.kotlin.gradle.dsl.JvmTarget

allprojects {
    group = "org.dynadoc"
    version = "2.0.0"
}

plugins {
    // Apply the org.jetbrains.kotlin.jvm plugin to add support for Kotlin.
    kotlin("jvm") version "2.3.21"
    id("org.jetbrains.kotlinx.kover") version "0.8.0"
    id("maven-publish")
    id("signing")
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlinx.kover")
    // Apply the java-library plugin for API and implementation separation.
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    kotlin {
        compilerOptions {
            jvmTarget = JvmTarget.JVM_1_8
        }
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_1_8
    }

    repositories {
        mavenCentral()
    }

    tasks.named<Test>("test") {
        // Use JUnit Platform for unit tests.
        useJUnitPlatform()
    }

    configure<JavaPluginExtension> {
        withSourcesJar()
        withJavadocJar()
    }
}

repositories {
    mavenCentral()
}

dependencies {
    kover(project(":dynadoc"))
    kover(project(":dynadoc-kotlinx-serialization"))
    kover(project(":dynadoc-jackson"))
    kover(project(":dynadoc-moshi"))
}
