// BattlePassServer.java
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.sql.*;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BattlePassServer {
    private static final Logger logger = LoggerFactory.getLogger(BattlePassServer.class);
    private static final int PORT = 8888;
    private static final int MAX_THREADS = 200;
    
    private static HikariDataSource dataSource;
    private static JedisPool redisPool;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static ExecutorService pool;

    static {
        try {
            initializeConfigurations();
            initializeDatabase();
            pool = Executors.newFixedThreadPool(MAX_THREADS);
        } catch (Exception e) {
            logger.error("Initialization failed", e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown()));
        
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            logger.info("Server started on port {}", PORT);
            while (!Thread.currentThread().isInterrupted()) {
                Socket clientSocket = serverSocket.accept();
                pool.execute(new ClientHandler(clientSocket));
            }
        } catch (IOException e) {
            logger.error("Server error", e);
        }
    }

    private static void initializeConfigurations() {
        try (InputStream input = BattlePassServer.class.getResourceAsStream("/config.properties")) {
            Properties prop = new Properties();
            prop.load(input);

            // Database setup
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl(prop.getProperty("db.url"));
            hikariConfig.setUsername(prop.getProperty("db.user"));
            hikariConfig.setPassword(prop.getProperty("db.password"));
            hikariConfig.setMaximumPoolSize(Integer.parseInt(prop.getProperty("db.pool.size")));
            dataSource = new HikariDataSource(hikariConfig);

            // Redis setup
            JedisPoolConfig jedisConfig = new JedisPoolConfig();
            jedisConfig.setMaxTotal(Integer.parseInt(prop.getProperty("redis.pool.size")));
            redisPool = new JedisPool(jedisConfig,
                prop.getProperty("redis.host"),
                Integer.parseInt(prop.getProperty("redis.port")));

        } catch (IOException e) {
            throw new RuntimeException("Configuration loading failed", e);
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket socket;

        ClientHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                
                String request;
                while ((request = in.readLine()) != null) {
                    processRequest(request, out);
                }
            } catch (IOException e) {
                logger.warn("Client connection error", e);
            } finally {
                closeSocket();
            }
        }

        private void processRequest(String request, PrintWriter out) {
            try {
                Command cmd = objectMapper.readValue(request, Command.class);
                validateCommand(cmd);

                switch (cmd.getType()) {
                    case "LOAD" -> handleLoad(cmd, out);
                    case "SAVE" -> handleSave(cmd);
                    case "EVENT" -> handleEvent(cmd, out);
                    case "PING" -> sendResponse(out, new Response("PONG", cmd.getPlayerId()));
                    default -> throw new IllegalArgumentException("Unknown command");
                }
            } catch (Exception e) {
                sendError(out, e, cmd != null ? cmd.getPlayerId() : -1);
            }
        }

        private void handleLoad(Command cmd, PrintWriter out) throws Exception {
            BattlePass bp = loadPlayerData(cmd.getPlayerId());
            sendResponse(out, new BattlePassResponse(
                cmd.getPlayerId(),
                bp.getCurrentLevel(),
                bp.getCurrentXp(),
                bp.getQuestProgress()
            ));
        }

        private void handleSave(Command cmd) throws Exception {
            BattlePass bp = objectMapper.convertValue(cmd.getData(), BattlePass.class);
            savePlayerData(cmd.getPlayerId(), bp);
        }

        private void handleEvent(Command cmd, PrintWriter out) throws Exception {
            BattlePass bp = loadPlayerData(cmd.getPlayerId());
            processEvent(bp, cmd.getEventType(), cmd.getAmount());
            savePlayerData(cmd.getPlayerId(), bp);
            sendResponse(out, new BattlePassResponse(
                cmd.getPlayerId(),
                bp.getCurrentLevel(),
                bp.getCurrentXp(),
                bp.getQuestProgress()
            ));
        }

        private void processEvent(BattlePass bp, String eventType, int amount) {
            Quest quest = bp.getQuests().get(eventType);
            if (quest != null) {
                quest.updateProgress(amount);
                checkLevelUps(bp);
            }
        }

        private void validateCommand(Command cmd) {
            if (cmd.getPlayerId() <= 0) {
                throw new IllegalArgumentException("Invalid player ID");
            }
        }

        private void sendResponse(PrintWriter out, Response response) {
            try {
                out.println(objectMapper.writeValueAsString(response));
            } catch (JsonProcessingException e) {
                logger.error("Response serialization failed", e);
            }
        }

        private void sendError(PrintWriter out, Exception e, int playerId) {
            logger.error("Request processing error", e);
            sendResponse(out, new Response("ERROR", playerId).withMessage(e.getMessage()));
        }

        private void closeSocket() {
            try {
                if (!socket.isClosed()) socket.close();
            } catch (IOException e) {
                logger.warn("Socket close error", e);
            }
        }
    }

    // Database operations
    private static BattlePass loadPlayerData(int playerId) throws Exception {
        // Cache lookup
        try (var jedis = redisPool.getResource()) {
            String cached = jedis.get("bp:" + playerId);
            if (cached != null) {
                return objectMapper.readValue(cached, BattlePass.class);
            }
        }

        // Database lookup
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 "SELECT data FROM passes WHERE player_id = ?")) {
            
            stmt.setInt(1, playerId);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next()) {
                BattlePass bp = objectMapper.readValue(rs.getString("data"), BattlePass.class);
                updateCache(playerId, bp);
                return bp;
            }
        }

        // New player
        BattlePass newBp = BattlePass.createDefault(loadDefaultLevels());
        savePlayerData(playerId, newBp);
        return newBp;
    }

    private static void savePlayerData(int playerId, BattlePass bp) throws Exception {
        String data = objectMapper.writeValueAsString(bp);
        
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            
            try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO passes (player_id, data) VALUES (?, ?) " +
                "ON DUPLICATE KEY UPDATE data = ?, last_updated = NOW()")) {
                
                stmt.setInt(1, playerId);
                stmt.setString(2, data);
                stmt.setString(3, data);
                stmt.executeUpdate();
                conn.commit();
                
                updateCache(playerId, bp);
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
    }

    private static void checkLevelUps(BattlePass bp) {
        int levelsGained = bp.calculateLevelProgress();
        if (levelsGained > 0) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                     "INSERT INTO rewards (player_id, level, reward) VALUES (?, ?, ?)")) {
                
                for (int i = 1; i <= levelsGained; i++) {
                    stmt.setInt(1, bp.getPlayerId());
                    stmt.setInt(2, bp.getCurrentLevel() + i);
                    stmt.setString(3, bp.getRewardForLevel(bp.getCurrentLevel() + i));
                    stmt.addBatch();
                }
                stmt.executeBatch();
            } catch (SQLException e) {
                logger.error("Reward saving failed", e);
            }
        }
    }

    private static void updateCache(int playerId, BattlePass bp) {
        try (var jedis = redisPool.getResource()) {
            jedis.setex("bp:" + playerId, 1800, objectMapper.writeValueAsString(bp));
        } catch (Exception e) {
            logger.warn("Cache update failed", e);
        }
    }

    private static void initializeDatabase() {
        try (Connection conn = dataSource.getConnection()) {
            conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS passes (" +
                "player_id INT PRIMARY KEY, " +
                "data TEXT NOT NULL, " +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
            
            conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS rewards (" +
                "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                "player_id INT NOT NULL, " +
                "level INT NOT NULL, " +
                "reward VARCHAR(255) NOT NULL, " +
                "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                "INDEX idx_player (player_id))");
        } catch (SQLException e) {
            throw new RuntimeException("Database initialization failed", e);
        }
    }

    private static void shutdown() {
        logger.info("Shutting down server...");
        pool.shutdown();
        dataSource.close();
        redisPool.close();
    }

    private static List<Level> loadDefaultLevels() {
        return List.of(
            new Level(1, 100, "Начальный меч"),
            new Level(2, 300, "Зелье здоровья x3"),
            new Level(3, 600, "Золотой набор брони"),
            new Level(4, 1000, "Легендарное оружие")
        );
    }

    // DTO Classes
    static class Command {
        private String type;
        private int playerId;
        private String eventType;
        private int amount;
        private Map<String, Object> data;
        // Getters & Setters
    }

    static class Response {
        private String status;
        private int playerId;
        private String message;
        // Getters & Setters
    }

    static class BattlePassResponse extends Response {
        private int level;
        private int xp;
        private Map<String, Integer> quests;
        // Constructor & Getters
    }
                                                     }
