/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.demos.twitter;

import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.algo.WindowedTopCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;

/**
 * Twitter Demo Application: <br>
 * This demo application samples random public status from twitter, send to url
 * extractor. <br>
 * Top 10 url(s) mentioned in tweets in last 1 mins are displayed oin every
 * window count (500ms). <br>
 * <br>
 * 
 * Real Time Calculation : <br>
 * This application calculates top 10 url mentioned in tweets in last one
 * minute across random tweet sampling. <br>
 * <br>
 * Before running this application, you need to create a Twitter API account
 * and configure the Twitter authentication.
 * For the CLI, those go into ~/.stram/stram-site.xml:
 * <pre>
 * {@code
 * <?xml version="1.0" encoding="UTF-8"?>
 * <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
 * <configuration>
 *
 *   <property> <name>stram.operator.TweetSampler.consumerKey</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>stram.operator.TweetSampler.consumerSecret</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>stram.operator.TweetSampler.accessToken</name>
 *   <value>TBD</value> </property>
 *
 *   <property> <name>stram.operator.TweetSampler.accessTokenSecret</name>
 *   <value>TBD</value> </property>
 * </configuration>
 * }
 * </pre>
 * Custom Attributes: <br>
 * <b>topCounts operator : <b>
 * <ul>
 * <li>Top Count : 10, number of top unique url to be reported.</li>
 * <li>Sliding window count : 300 , report over last 5 min</li>
 * <li>window slide value : 1</li>
 * </ul>
 * <p>
 * Running Java Test or Main app in IDE:
 * 
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 * 
 * Run Success : <br>
 * For successful deployment and run, user should see following output on
 * console:
 * 
 * <pre>
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * topURLs: {http://goo.gl/V0R05=2, http://etsy.me/10r1Yg3=6, http://tinyurl.com/88b5jqb=2, http://www.justunfollow.com=4, http://fllwrs.com=2, http://goo.gl/a9Sjp=2, http://goo.gl/iKeVH=2, http://Unfollowers.me=7, http://freetexthost.com/j3y03la4g3=2, http://uranaitter.com=4}
 * 2013-06-17 14:38:55,201 [main] INFO  stram.StramLocalCluster run - Application finished.
 * 2013-06-17 14:38:55,201 [container-2] INFO  stram.StramChild processHeartbeatResponse - Received shutdown request
 * </pre>
 * 
 * Scaling Options : <br>
 * User can scale application by setting intial partition size > 1 on count
 * unique operator. <br>
 * <br>
 * 
 * Application DAG : <br>
 * <img src="doc-files/Application.gif" width=600px > <br>
 * <br>
 * 
 * Streaming Window Size : 500ms(default) <br>
 * Operator Details : <br>
 * <ul>
 * <li><b>The twitterFeed operator : </b> This operator samples random public
 * statues from twitter and emits to application. <br>
 * Class : com.datatorrent.demos.twitter.TwitterSampleInput <br>
 * State Less : YES, window count 1 <br>
 * </li>
 * <li><b>The urlExtractor operator : </b> This operator extracts url from
 * random sampled statues from twitter. <br>
 * Class : {@link com.datatorrent.demos.twitter.TwitterStatusURLExtractor} <br>
 * State Less : Yes, window count 1 <br>
 * </li>
 * <li><b>The uniqueCounter operator : </b> This operator aggregates count for each
 * url extracted from random samples. <br>
 * Class : {@link com.datatorrent.lib.algo.UniqueCounter} <br>
 * State Less : YES, window count 1 <br>
 * </li>
 * <li><b> The topCounts operator : </b> This operator caluculates top url in last 1
 * min sliding window count 1. <br>
 * Class : com.datatorrent.lib.algo.WindowedTopCounter <br>
 * State Full : Yes, sliding window count 120 (1 min) <br>
 * </li>
 * <li><b>The operator Console: </b> This operator just outputs the input tuples
 * to the console (or stdout). <br>
 * if you need to change write to HDFS,HTTP .. instead of console, <br>
 * Please refer to {@link com.datatorrent.lib.io.HttpOutputOperator} or
 * {@link com.datatorrent.lib.io.HdfsOutputOperator}.</li>
 * </ul>
 * 
 */
public class TwitterTopCounterApplication implements StreamingApplication
{
  private static final boolean inline = false;

  private InputPort<Object> consoleOutput(DAG dag, String operatorName)
  {
    String daemonAddress = dag.attrValue(DAG.DAEMON_ADDRESS, null);
    if (!StringUtils.isEmpty(daemonAddress)) {
      URI uri = URI.create("ws://" + daemonAddress + "/pubsub");
      String topic = "demos.twitter." + operatorName;
      //LOG.info("WebSocket with daemon at: {}", daemonAddress);
      PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator(operatorName, new PubSubWebSocketOutputOperator<Object>());
      wsOut.setUri(uri);
      wsOut.setTopic(topic);
      return wsOut.input;
    }
    ConsoleOutputOperator operator = dag.addOperator(operatorName, new ConsoleOutputOperator());
    operator.setStringFormat(operatorName + ": %s");
    return operator.input;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.APPLICATION_NAME, "TwitterDevApplication");

    // Setup the operator to get the data from twitter sample stream injected into the system.
    TwitterSampleInput twitterFeed = new TwitterSampleInput();
    twitterFeed = dag.addOperator("TweetSampler", twitterFeed);
    
    //  Setup the operator to get the URLs extracted from the twitter statuses
    TwitterStatusURLExtractor urlExtractor = dag.addOperator("URLExtractor", TwitterStatusURLExtractor.class);

    // Setup a node to count the unique urls within a window.
    UniqueCounter<String> uniqueCounter = dag.addOperator("UniqueURLCounter", new UniqueCounter<String>());

    // Get the aggregated url counts and count them over last 5 mins.
    WindowedTopCounter<String> topCounts = dag.addOperator("TopCounter", new WindowedTopCounter<String>());
    topCounts.setTopCount(10);
    topCounts.setSlidingWindowWidth(600, 1);

    // Feed the statuses from feed into the input of the url extractor.
    dag.addStream("TweetStream", twitterFeed.status, urlExtractor.input).setInline(true);
    //  Start counting the urls coming out of URL extractor
    dag.addStream("TwittedURLs", urlExtractor.url, uniqueCounter.data).setInline(inline);
    // Count unique urls
    dag.addStream("UniqueURLCounts", uniqueCounter.count, topCounts.input).setInline(inline);
    // Count top 10
    dag.addStream("TopURLs", topCounts.output, consoleOutput(dag, "topURLs")).setInline(inline);

  }

}
