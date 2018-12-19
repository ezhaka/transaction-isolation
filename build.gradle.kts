import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.avast.gradle.dockercompose.*

buildscript {
    repositories {
        gradlePluginPortal()
    }
    dependencies {
        classpath("com.avast.gradle:gradle-docker-compose-plugin:0.8.12")
    }
}

apply(plugin = "docker-compose")

plugins {
    kotlin("jvm") version "1.3.11"
}

group = "seal.happy"
version = "1.0-SNAPSHOT"

repositories {
    jcenter()
    mavenCentral()
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    compile("org.jetbrains.exposed:exposed:0.11.2")
    compile("mysql:mysql-connector-java:5.1.6")

    testCompile("org.junit.jupiter:junit-jupiter-api:5.3.2")
    testCompile("org.junit.jupiter:junit-jupiter-params:5.3.2")
    testRuntime("org.junit.jupiter:junit-jupiter-engine:5.3.2")
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<Test> {
    useJUnitPlatform()
}

configure<ComposeExtension> {
    isRequiredBy(tasks.getByName("test"))
}