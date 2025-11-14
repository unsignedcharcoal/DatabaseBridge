plugins {
    id("java")
}

group = "dev.charcoal.database.bridge"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.github.ben-manes.caffeine:caffeine:3.2.3") //cache

    implementation("io.lettuce:lettuce-core:7.0.0.RELEASE") //cloud cache
    implementation("org.mongodb:mongodb-driver-sync:5.6.1") //cloud persistent
    implementation("com.zaxxer:HikariCP:7.0.2") //mysql
    implementation("com.mysql:mysql-connector-j:9.1.0")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.20.0") //json
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.20.1")
    implementation("org.yaml:snakeyaml:2.5") //yaml


    compileOnly("org.projectlombok:lombok:1.18.40")
    annotationProcessor("org.projectlombok:lombok:1.18.40")
}

tasks.test {
    useJUnitPlatform()
}