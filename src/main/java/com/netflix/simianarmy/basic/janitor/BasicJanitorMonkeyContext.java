package com.netflix.simianarmy.basic.janitor;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.simianarmy.MonkeyCalendar;
import com.netflix.simianarmy.MonkeyConfiguration;
import com.netflix.simianarmy.MonkeyRecorder;
import com.netflix.simianarmy.aws.janitor.ASGJanitor;
import com.netflix.simianarmy.aws.janitor.EBSSnapshotJanitor;
import com.netflix.simianarmy.aws.janitor.EBSVolumeJanitor;
import com.netflix.simianarmy.aws.janitor.ImageJanitor;
import com.netflix.simianarmy.aws.janitor.InstanceJanitor;
import com.netflix.simianarmy.aws.janitor.LaunchConfigJanitor;
import com.netflix.simianarmy.aws.janitor.SimpleDBJanitorResourceTracker;
import com.netflix.simianarmy.aws.janitor.crawler.ASGJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.EBSSnapshotJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.EBSVolumeJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.InstanceJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.LaunchConfigJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.edda.EddaASGJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.edda.EddaEBSSnapshotJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.edda.EddaEBSVolumeJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.edda.EddaImageJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.edda.EddaInstanceJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.crawler.edda.EddaLaunchConfigJanitorCrawler;
import com.netflix.simianarmy.aws.janitor.rule.ami.UnusedImageRule;
import com.netflix.simianarmy.aws.janitor.rule.asg.ASGInstanceValidator;
import com.netflix.simianarmy.aws.janitor.rule.asg.DiscoveryASGInstanceValidator;
import com.netflix.simianarmy.aws.janitor.rule.asg.DummyASGInstanceValidator;
import com.netflix.simianarmy.aws.janitor.rule.asg.OldEmptyASGRule;
import com.netflix.simianarmy.aws.janitor.rule.asg.SuspendedASGRule;
import com.netflix.simianarmy.aws.janitor.rule.instance.OrphanedInstanceRule;
import com.netflix.simianarmy.aws.janitor.rule.launchconfig.OldUnusedLaunchConfigRule;
import com.netflix.simianarmy.aws.janitor.rule.snapshot.NoGeneratedAMIRule;
import com.netflix.simianarmy.aws.janitor.rule.volume.DeleteOnTerminationRule;
import com.netflix.simianarmy.aws.janitor.rule.volume.OldDetachedVolumeRule;
import com.netflix.simianarmy.basic.BasicSimianArmyContext;
import com.netflix.simianarmy.client.edda.EddaClient;
import com.netflix.simianarmy.janitor.AbstractJanitor;
import com.netflix.simianarmy.janitor.JanitorCrawler;
import com.netflix.simianarmy.janitor.JanitorEmailBuilder;
import com.netflix.simianarmy.janitor.JanitorEmailNotifier;
import com.netflix.simianarmy.janitor.JanitorMonkey;
import com.netflix.simianarmy.janitor.JanitorResourceTracker;
import com.netflix.simianarmy.janitor.JanitorRuleEngine;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
/** 
 * The basic implementation of the context class for Janitor monkey.
 */
public class BasicJanitorMonkeyContext extends BasicSimianArmyContext implements JanitorMonkey.Context {
  /** 
 * The Constant LOGGER. 
 */
  public static Logger LOGGER=LoggerFactory.getLogger(BasicJanitorMonkeyContext.class);
  /** 
 * The email notifier. 
 */
  public JanitorEmailNotifier emailNotifier;
  public JanitorResourceTracker janitorResourceTracker;
  /** 
 * The janitors. 
 */
  public List<AbstractJanitor> janitors;
  public String monkeyRegion;
  public MonkeyCalendar monkeyCalendar;
  public AmazonSimpleEmailServiceClient sesClient;
  public JanitorEmailBuilder janitorEmailBuilder;
  public String defaultEmail;
  public String[] ccEmails;
  public String sourceEmail;
  public String ownerEmailDomain;
  public int daysBeforeTermination;
  /** 
 * The constructor.
 */
  public BasicJanitorMonkeyContext(){
    super("simianarmy.properties","client.properties","janitor.properties");
    monkeyRegion=region();
    monkeyCalendar=calendar();
    String resourceDomain=configuration().getStrOrElse("simianarmy.janitor.resources.sdb.domain","SIMIAN_ARMY");
    Set<String> enabledResourceSet=getEnabledResourceSet();
    janitorResourceTracker=new SimpleDBJanitorResourceTracker(awsClient(),resourceDomain);
    janitorEmailBuilder=new BasicJanitorEmailBuilder();
    sesClient=new AmazonSimpleEmailServiceClient();
    defaultEmail=configuration().getStrOrElse("simianarmy.janitor.notification.defaultEmail","");
    ccEmails=StringUtils.split(configuration().getStrOrElse("simianarmy.janitor.notification.ccEmails",""),",");
    sourceEmail=configuration().getStrOrElse("simianarmy.janitor.notification.sourceEmail","");
    ownerEmailDomain=configuration().getStrOrElse("simianarmy.janitor.notification.ownerEmailDomain","");
    daysBeforeTermination=(int)configuration().getNumOrElse("simianarmy.janitor.notification.daysBeforeTermination",3);
    emailNotifier=new JanitorEmailNotifier(getJanitorEmailNotifierContext());
    janitors=new ArrayList<AbstractJanitor>();
    if (enabledResourceSet.contains("ASG")) {
      janitors.add(getASGJanitor());
    }
    if (enabledResourceSet.contains("INSTANCE")) {
      janitors.add(getInstanceJanitor());
    }
    if (enabledResourceSet.contains("EBS_VOLUME")) {
      janitors.add(getEBSVolumeJanitor());
    }
    if (enabledResourceSet.contains("EBS_SNAPSHOT")) {
      janitors.add(getEBSSnapshotJanitor());
    }
    if (enabledResourceSet.contains("LAUNCH_CONFIG")) {
      janitors.add(getLaunchConfigJanitor());
    }
    if (enabledResourceSet.contains("IMAGE")) {
      janitors.add(getImageJanitor());
    }
  }
  public ASGJanitor getASGJanitor(){
    JanitorRuleEngine ruleEngine=new BasicJanitorRuleEngine();
    boolean discoveryEnabled=configuration().getBoolOrElse("simianarmy.janitor.Eureka.enabled",false);
    ASGInstanceValidator instanceValidator;
    if (discoveryEnabled) {
      LOGGER.info("Initializing Discovery client.");
      instanceValidator=new DiscoveryASGInstanceValidator(DiscoveryManager.getInstance().getDiscoveryClient());
    }
 else {
      LOGGER.info("Discovery/Eureka is not enabled, use the dummy instance validator.");
      instanceValidator=new DummyASGInstanceValidator();
    }
    if (configuration().getBoolOrElse("simianarmy.janitor.rule.oldEmptyASGRule.enabled",false)) {
      ruleEngine.addRule(new OldEmptyASGRule(monkeyCalendar,(int)configuration().getNumOrElse("simianarmy.janitor.rule.oldEmptyASGRule.launchConfigAgeThreshold",50),(int)configuration().getNumOrElse("simianarmy.janitor.rule.oldEmptyASGRule.retentionDays",10),instanceValidator));
    }
    if (configuration().getBoolOrElse("simianarmy.janitor.rule.suspendedASGRule.enabled",false)) {
      ruleEngine.addRule(new SuspendedASGRule(monkeyCalendar,(int)configuration().getNumOrElse("simianarmy.janitor.rule.suspendedASGRule.suspensionAgeThreshold",2),(int)configuration().getNumOrElse("simianarmy.janitor.rule.suspendedASGRule.retentionDays",5),instanceValidator));
    }
    JanitorCrawler crawler;
    if (configuration().getBoolOrElse("simianarmy.janitor.edda.enabled",false)) {
      crawler=new EddaASGJanitorCrawler(createEddaClient(),awsClient().region());
    }
 else {
      crawler=new ASGJanitorCrawler(awsClient());
    }
    BasicJanitorContext asgJanitorCtx=new BasicJanitorContext(monkeyRegion,ruleEngine,crawler,janitorResourceTracker,monkeyCalendar,configuration(),recorder());
    return new ASGJanitor(awsClient(),asgJanitorCtx);
  }
  public InstanceJanitor getInstanceJanitor(){
    JanitorRuleEngine ruleEngine=new BasicJanitorRuleEngine();
    if (configuration().getBoolOrElse("simianarmy.janitor.rule.orphanedInstanceRule.enabled",false)) {
      ruleEngine.addRule(new OrphanedInstanceRule(monkeyCalendar,(int)configuration().getNumOrElse("simianarmy.janitor.rule.orphanedInstanceRule.instanceAgeThreshold",2),(int)configuration().getNumOrElse("simianarmy.janitor.rule.orphanedInstanceRule.retentionDaysWithOwner",3),(int)configuration().getNumOrElse("simianarmy.janitor.rule.orphanedInstanceRule.retentionDaysWithoutOwner",8)));
    }
    JanitorCrawler instanceCrawler;
    if (configuration().getBoolOrElse("simianarmy.janitor.edda.enabled",false)) {
      instanceCrawler=new EddaInstanceJanitorCrawler(createEddaClient(),awsClient().region());
    }
 else {
      instanceCrawler=new InstanceJanitorCrawler(awsClient());
    }
    BasicJanitorContext instanceJanitorCtx=new BasicJanitorContext(monkeyRegion,ruleEngine,instanceCrawler,janitorResourceTracker,monkeyCalendar,configuration(),recorder());
    return new InstanceJanitor(awsClient(),instanceJanitorCtx);
  }
  public EBSVolumeJanitor getEBSVolumeJanitor(){
    JanitorRuleEngine ruleEngine=new BasicJanitorRuleEngine();
    if (configuration().getBoolOrElse("simianarmy.janitor.rule.oldDetachedVolumeRule.enabled",false)) {
      ruleEngine.addRule(new OldDetachedVolumeRule(monkeyCalendar,(int)configuration().getNumOrElse("simianarmy.janitor.rule.oldDetachedVolumeRule.detachDaysThreshold",30),(int)configuration().getNumOrElse("simianarmy.janitor.rule.oldDetachedVolumeRule.retentionDays",7)));
      if (configuration().getBoolOrElse("simianarmy.janitor.edda.enabled",false) && configuration().getBoolOrElse("simianarmy.janitor.rule.deleteOnTerminationRule.enabled",false)) {
        ruleEngine.addRule(new DeleteOnTerminationRule(monkeyCalendar,(int)configuration().getNumOrElse("simianarmy.janitor.rule.deleteOnTerminationRule.retentionDays",3)));
      }
    }
    JanitorCrawler volumeCrawler;
    if (configuration().getBoolOrElse("simianarmy.janitor.edda.enabled",false)) {
      volumeCrawler=new EddaEBSVolumeJanitorCrawler(createEddaClient(),awsClient().region());
    }
 else {
      volumeCrawler=new EBSVolumeJanitorCrawler(awsClient());
    }
    BasicJanitorContext volumeJanitorCtx=new BasicJanitorContext(monkeyRegion,ruleEngine,volumeCrawler,janitorResourceTracker,monkeyCalendar,configuration(),recorder());
    return new EBSVolumeJanitor(awsClient(),volumeJanitorCtx);
  }
  public EBSSnapshotJanitor getEBSSnapshotJanitor(){
    JanitorRuleEngine ruleEngine=new BasicJanitorRuleEngine();
    if (configuration().getBoolOrElse("simianarmy.janitor.rule.noGeneratedAMIRule.enabled",false)) {
      ruleEngine.addRule(new NoGeneratedAMIRule(monkeyCalendar,(int)configuration().getNumOrElse("simianarmy.janitor.rule.noGeneratedAMIRule.ageThreshold",30),(int)configuration().getNumOrElse("simianarmy.janitor.rule.noGeneratedAMIRule.retentionDays",7)));
    }
    JanitorCrawler snapshotCrawler;
    if (configuration().getBoolOrElse("simianarmy.janitor.edda.enabled",false)) {
      snapshotCrawler=new EddaEBSSnapshotJanitorCrawler(configuration().getStr("simianarmy.janitor.snapshots.ownerId"),createEddaClient(),awsClient().region());
    }
 else {
      snapshotCrawler=new EBSSnapshotJanitorCrawler(awsClient());
    }
    BasicJanitorContext snapshotJanitorCtx=new BasicJanitorContext(monkeyRegion,ruleEngine,snapshotCrawler,janitorResourceTracker,monkeyCalendar,configuration(),recorder());
    return new EBSSnapshotJanitor(awsClient(),snapshotJanitorCtx);
  }
  public LaunchConfigJanitor getLaunchConfigJanitor(){
    JanitorRuleEngine ruleEngine=new BasicJanitorRuleEngine();
    if (configuration().getBoolOrElse("simianarmy.janitor.rule.oldUnusedLaunchConfigRule.enabled",false)) {
      ruleEngine.addRule(new OldUnusedLaunchConfigRule(monkeyCalendar,(int)configuration().getNumOrElse("simianarmy.janitor.rule.oldUnusedLaunchConfigRule.ageThreshold",4),(int)configuration().getNumOrElse("simianarmy.janitor.rule.oldUnusedLaunchConfigRule.retentionDays",3)));
    }
    JanitorCrawler crawler;
    if (configuration().getBoolOrElse("simianarmy.janitor.edda.enabled",false)) {
      crawler=new EddaLaunchConfigJanitorCrawler(createEddaClient(),awsClient().region());
    }
 else {
      crawler=new LaunchConfigJanitorCrawler(awsClient());
    }
    BasicJanitorContext janitorCtx=new BasicJanitorContext(monkeyRegion,ruleEngine,crawler,janitorResourceTracker,monkeyCalendar,configuration(),recorder());
    return new LaunchConfigJanitor(awsClient(),janitorCtx);
  }
  public ImageJanitor getImageJanitor(){
    JanitorCrawler crawler;
    if (configuration().getBoolOrElse("simianarmy.janitor.edda.enabled",false)) {
      crawler=new EddaImageJanitorCrawler(createEddaClient(),configuration().getStr("simianarmy.janitor.image.ownerId"),(int)configuration().getNumOrElse("simianarmy.janitor.image.crawler.lookBackDays",60),awsClient().region());
    }
 else {
      throw new RuntimeException("Image Janitor only works when Edda is enabled.");
    }
    JanitorRuleEngine ruleEngine=new BasicJanitorRuleEngine();
    if (configuration().getBoolOrElse("simianarmy.janitor.rule.unusedImageRule.enabled",false)) {
      ruleEngine.addRule(new UnusedImageRule(monkeyCalendar,(int)configuration().getNumOrElse("simianarmy.janitor.rule.unusedImageRule.retentionDays",3),(int)configuration().getNumOrElse("simianarmy.janitor.rule.unusedImageRule.lastReferenceDaysThreshold",45)));
    }
    BasicJanitorContext janitorCtx=new BasicJanitorContext(monkeyRegion,ruleEngine,crawler,janitorResourceTracker,monkeyCalendar,configuration(),recorder());
    return new ImageJanitor(awsClient(),janitorCtx);
  }
  public EddaClient createEddaClient(){
    return new EddaClient((int)configuration().getNumOrElse("simianarmy.janitor.edda.client.timeout",30000),(int)configuration().getNumOrElse("simianarmy.janitor.edda.client.retries",3),(int)configuration().getNumOrElse("simianarmy.janitor.edda.client.retryInterval",1000),configuration());
  }
  public Set<String> getEnabledResourceSet(){
    Set<String> enabledResourceSet=new HashSet<String>();
    String enabledResources=configuration().getStr("simianarmy.janitor.enabledResources");
    if (StringUtils.isNotBlank(enabledResources)) {
      for (      String resourceType : enabledResources.split(",")) {
        enabledResourceSet.add(resourceType.trim().toUpperCase());
      }
    }
    return enabledResourceSet;
  }
  public JanitorEmailNotifier.Context getJanitorEmailNotifierContext(){
    return new JanitorEmailNotifier.Context(){
      @Override public AmazonSimpleEmailServiceClient sesClient(){
        return sesClient;
      }
      @Override public String defaultEmail(){
        return defaultEmail;
      }
      @Override public int daysBeforeTermination(){
        return daysBeforeTermination;
      }
      @Override public String region(){
        return monkeyRegion;
      }
      @Override public JanitorResourceTracker resourceTracker(){
        return janitorResourceTracker;
      }
      @Override public JanitorEmailBuilder emailBuilder(){
        return janitorEmailBuilder;
      }
      @Override public MonkeyCalendar calendar(){
        return monkeyCalendar;
      }
      @Override public String[] ccEmails(){
        return ccEmails;
      }
      @Override public String sourceEmail(){
        return sourceEmail;
      }
      @Override public String ownerEmailDomain(){
        return ownerEmailDomain;
      }
    }
;
  }
  /** 
 * {@inheritDoc} 
 */
  @Override public List<AbstractJanitor> janitors(){
    return janitors;
  }
  /** 
 * {@inheritDoc} 
 */
  @Override public JanitorEmailNotifier emailNotifier(){
    return emailNotifier;
  }
  @Override public JanitorResourceTracker resourceTracker(){
    return janitorResourceTracker;
  }
  /** 
 * The Context class for Janitor.
 */
public static class BasicJanitorContext implements AbstractJanitor.Context {
    public String region;
    public JanitorRuleEngine ruleEngine;
    public JanitorCrawler crawler;
    public JanitorResourceTracker resourceTracker;
    public MonkeyCalendar calendar;
    public MonkeyConfiguration config;
    public MonkeyRecorder recorder;
    /** 
 * Constructor.
 * @param region the region of the janitor
 * @param ruleEngine the rule engine used by the janitor
 * @param crawler the crawler used by the janitor
 * @param resourceTracker the resource tracker used by the janitor
 * @param calendar the calendar used by the janitor
 * @param config the monkey configuration used by the janitor
 */
    public BasicJanitorContext(    String region,    JanitorRuleEngine ruleEngine,    JanitorCrawler crawler,    JanitorResourceTracker resourceTracker,    MonkeyCalendar calendar,    MonkeyConfiguration config,    MonkeyRecorder recorder){
      this.region=region;
      this.resourceTracker=resourceTracker;
      this.ruleEngine=ruleEngine;
      this.crawler=crawler;
      this.calendar=calendar;
      this.config=config;
      this.recorder=recorder;
    }
    @Override public String region(){
      return region;
    }
    @Override public MonkeyConfiguration configuration(){
      return config;
    }
    @Override public MonkeyCalendar calendar(){
      return calendar;
    }
    @Override public JanitorRuleEngine janitorRuleEngine(){
      return ruleEngine;
    }
    @Override public JanitorCrawler janitorCrawler(){
      return crawler;
    }
    @Override public JanitorResourceTracker janitorResourceTracker(){
      return resourceTracker;
    }
    @Override public MonkeyRecorder recorder(){
      return recorder;
    }
    public BasicJanitorContext(){
    }
  }
}
