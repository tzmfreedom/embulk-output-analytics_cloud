Embulk::JavaPlugin.register_output(
  "analytics_cloud", "org.embulk.output.analytics_cloud.AnalyticsCloudOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
