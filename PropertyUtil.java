public class PropertyUtil {

    private static final Properties props = new Properties();

    static {
        try {
            // Load base
            try (InputStream base = PropertyUtil.class.getClassLoader()
                    .getResourceAsStream("application.properties")) {
                if (base != null) props.load(base);
            }

            // Check for profile
            String profile = System.getProperty("spring.profiles.active",
                    System.getenv("SPRING_PROFILES_ACTIVE"));

            if (profile != null) {
                try (InputStream prof = PropertyUtil.class.getClassLoader()
                        .getResourceAsStream("application-" + profile + ".properties")) {
                    if (prof != null) props.load(prof);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not load properties", e);
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }
}
