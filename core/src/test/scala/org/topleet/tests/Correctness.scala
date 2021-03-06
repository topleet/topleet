package org.topleet.tests

import org.scalatest.featurespec.AnyFeatureSpec
import org.topleet.Engine
import org.topleet.engines.ReferenceEngine

trait Correctness extends AnyFeatureSpec {

  def reference(): Engine = ReferenceEngine

  def engines(): Map[String, () => Engine]

  def Applications = ???

  // TODO: This does not work as these guys are in git.
  def Repositories: Seq[(String, Int)] =
    Seq(
      ("Yalantis/Side-Menu.Android", 62),
      ("googlesamples/android-play-billing", 68),
      ("stleary/JSON-java", 92),
      ("zhihu/Matisse", 160),
      ("google/iosched", 169),
      ("citerus/dddsample-core", 191),
      ("medcl/elasticsearch-analysis-ik", 202),
      ("anjlab/android-inapp-billing-v3", 203),
      ("square/otto", 204),
      ("kdn251/Interviews", 206),
      ("pilgr/Paper", 210),
      ("tbruyelle/RxPermissions", 218),
      ("googlesamples/android-testing", 224),
      ("junit-team/junit4", 226),
      ("ReactiveX/RxNetty", 250),
      ("stephanenicolas/robospice", 271),
      ("reactive-streams/reactive-streams-jvm", 281),
      ("Netflix/curator", 372),
      ("eishay/jvm-serializers", 398),
      ("apache/incubator-dubbo-ops", 399),
      ("powermock/powermock", 430),
      ("mybatis/spring", 473),
      ("eclipse-vertx/vert.x", 473),
      ("Netflix/archaius", 474),
      ("Netflix/astyanax", 475),
      ("eclipse/eclipse-collections", 513),
      ("qos-ch/slf4j", 531),
      ("facebook/stetho", 537),
      ("tobie/ua-parser", 541),
      ("termux/termux-app", 546),
      ("loopj/android-async-http", 576),
      ("shopizer-ecommerce/shopizer", 618),
      ("square/dagger", 647),
      ("eclipse/paho.mqtt.java", 651),
      ("Bukkit/Bukkit", 656),
      ("abel533/Mapper", 697),
      ("internetarchive/heritrix3", 723),
      ("functionaljava/functionaljava", 757),
      ("koush/AndroidAsync", 834),
      ("google/physical-web", 869),
      ("nhaarman/ListViewAnimations", 903),
      ("searchbox-io/Jest", 904),
      ("jberkel/sms-backup-plus", 949),
      ("rest-assured/rest-assured", 971),
      ("facebook/facebook-android-sdk", 974),
      ("alibaba/Sentinel", 1038),
      ("igniterealtime/Smack", 1051),
      ("web3j/web3j", 1059),
      ("apache/shiro", 1066),
      ("Netflix/Hystrix", 1159),
      ("bytedeco/javacpp-presets", 1159),
      ("spring-projects/spring-session", 1172),
      ("gwtproject/gwt", 1181),
      ("Microsoft/malmo", 1264),
      ("rzwitserloot/lombok", 1294),
      ("JetBrains/ideavim", 1322),
      ("tomakehurst/wiremock", 1332),
      ("google/flexbox-layout", 1375),
      ("tinkerpop/blueprints", 1455),
      ("linkedin/rest.li", 1490),
      ("orgzly/orgzly-android", 1534),
      ("syncthing/syncthing-android", 1549),
      ("westnordost/StreetComplete", 1557),
      ("androidannotations/androidannotations", 1577),
      ("micrometer-metrics/micrometer", 1675),
      ("yacy/yacy_search_server", 1807),
      ("apache/avro", 1910),
      ("apache/pulsar", 1961),
      ("alibaba/atlas", 1963),
      ("unclebob/fitnesse", 2026),
      //("Bukkit/CraftBukkit", 2189), (This requires credentials)
      ("debezium/debezium", 2233),
      ("Sable/soot", 2303),
      ("Deletescape-Media/Lawnchair", 2374),
      ("scouter-project/scouter", 2453),
      ("SecUpwN/Android-IMSI-Catcher-Detector", 2467),
      ("ctripcorp/apollo", 2596),
      ("nickbutcher/plaid", 2787),
      ("ehcache/ehcache3", 3336),
      ("querydsl/querydsl", 3445),
      ("apache/zookeeper", 3452),
      ("sparklemotion/nokogiri", 3523),
      ("twitter/heron", 3889),
      ("MinecraftForge/MinecraftForge", 5182),
      ("libgdx/libgdx", 6641),
      ("openhab/openhab", 6857),
      ("apache/kylin", 7472),
      ("wireapp/wire-android", 10597),
      ("cloudfoundry/uaa", 11574),
      ("groovy/groovy-core", 12071),
      ("linkedin/pinot", 12813),
      ("wildfly/wildfly", 13262),
      ("prestodb/presto", 14543),
      ("apache/cassandra", 15822),
      ("apache/flink", 19275),
      ("languagetool-org/languagetool", 20639),
      ("rstudio/rstudio", 23135),
      ("facebook/react-native", 25965),
      ("wordpress-mobile/WordPress-Android", 40123),
      ("SonarSource/sonarqube", 56356))

}
