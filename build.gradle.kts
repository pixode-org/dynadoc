import org.jetbrains.kotlin.gradle.dsl.JvmTarget

allprojects {
    group = "org.pixode"
    version = "3.0.0"
}

plugins {
    kotlin("jvm") version "2.3.21" apply false
    id("org.jetbrains.kotlinx.kover") version "0.9.8"
}

subprojects {
    pluginManager.apply("org.jetbrains.kotlin.jvm")
    pluginManager.apply("org.jetbrains.kotlinx.kover")
    pluginManager.apply("java-library")
    pluginManager.apply("maven-publish")
    pluginManager.apply("signing")

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
        compilerOptions {
            jvmTarget = JvmTarget.JVM_1_8
        }
    }

    pluginManager.withPlugin("java") {
        configure<JavaPluginExtension> {
            sourceCompatibility = JavaVersion.VERSION_1_8
            withSourcesJar()
            withJavadocJar()
        }
    }

    repositories {
        mavenCentral()
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }
}

repositories {
    mavenCentral()
}

dependencies {
    kover(project(":dynadoc"))
    kover(project(":dynadoc-jackson"))
}
