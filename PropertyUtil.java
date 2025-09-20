@Component
public class SpringPropertyBridge implements EnvironmentAware {

    private static Environment env;

    @Override
    public void setEnvironment(Environment environment) {
        env = environment;
    }

    public static String get(String key) {
        return env.getProperty(key);
    }
}
