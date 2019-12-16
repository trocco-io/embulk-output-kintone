Embulk::JavaPlugin.register_output(
  "kintone", "org.embulk.output.kintone.KintoneOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
