
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

const SUPABASE_URL =
  "https://orznejzevoclmjqfulzh.supabase.co";

const PUBLISHABLE_KEY =
  process.env.SUPABASE_PUBLISHABLE_KEY;

const SERVICE_KEY =
  process.env.SUPABASE_SERVICE_ROLE_KEY;

const TEST_USERS = new Set(
  [
    process.env.TEST_USER_1,
    process.env.TEST_USER_2
  ].filter(Boolean)
);

const CHAT_DURATION = 5 * 60 * 1000;

const waitingUsers = new Set();
const sessions = new Map();
const matchingUsers = new Set();

function adminHeaders() {
  if (!SERVICE_KEY) {
    throw new Error("Supabase sunucu anahtarı eksik.");
  }

  return {
    apikey: SERVICE_KEY,
    Authorization: `Bearer ${SERVICE_KEY}`,
    "Content-Type": "application/json"
  };
}

// Kullanıcı kimliğini Supabase üzerinden doğrula
async function verifyUser(token) {
  if (!token || typeof token !== "string") {
    return null;
  }

  try {
    const response = await fetch(
      `${SUPABASE_URL}/auth/v1/user`,
      {
        headers: {
          Authorization: `Bearer ${token}`,
          apikey: PUBLISHABLE_KEY || ""
        },
        signal: AbortSignal.timeout(8000)
      }
    );

    if (!response.ok) return null;

    const user = await response.json();

    if (!user.id || !TEST_USERS.has(user.id)) {
      return null;
    }

    return user;
  } catch (error) {
    console.error("Kimlik doğrulama:", error.message);
    return null;
  }
}

// Socket bağlantı güvenliği
io.use(async (socket, next) => {
  const token = socket.handshake.auth?.token;
  const user = await verifyUser(token);

  if (!user) {
    return next(
      new Error("Bu hesap test için yetkili değil.")
    );
  }

  const alreadyConnected =
    [...io.sockets.sockets.values()]
      .some(s => s.data.userId === user.id);

  if (alreadyConnected) {
    return next(
      new Error("Bu hesap zaten bağlı.")
    );
  }

  socket.data.userId = user.id;
  next();
});

// İki kullanıcı birbirini engellemiş mi?
async function isBlocked(userA, userB) {
  const params = new URLSearchParams();

  params.set("select", "id");
  params.set(
    "or",
    `and(blocker_id.eq.${userA},blocked_id.eq.${userB}),and(blocker_id.eq.${userB},blocked_id.eq.${userA})`
  );

  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/user_blocks?${params}`,
    {
      headers: adminHeaders(),
      signal: AbortSignal.timeout(8000)
    }
  );

  if (!response.ok) {
    throw new Error("Engelleme kontrolü başarısız.");
  }

  const rows = await response.json();

  return rows.length > 0;
}

// Kullanıcıyı engelle
async function saveBlock(blockerId, blockedId) {
  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/user_blocks?on_conflict=blocker_id,blocked_id`,
    {
      method: "POST",
      headers: {
        ...adminHeaders(),
        Prefer: "resolution=ignore-duplicates,return=minimal"
      },
      body: JSON.stringify({
        blocker_id: blockerId,
        blocked_id: blockedId
      }),
      signal: AbortSignal.timeout(8000)
    }
  );

  if (!response.ok) {
    throw new Error("Engelleme kaydedilemedi.");
  }
}

// Kullanıcı şikâyetini kaydet
async function saveReport(
  reporterId,
  reportedId,
  reason,
  details
) {
  const allowedReasons = [
    "harassment",
    "inappropriate_content",
    "spam",
    "other"
  ];

  if (!allowedReasons.includes(reason)) {
    throw new Error("Geçersiz şikâyet nedeni.");
  }

  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/user_reports`,
    {
      method: "POST",
      headers: {
        ...adminHeaders(),
        Prefer: "return=minimal"
      },
      body: JSON.stringify({
        reporter_id: reporterId,
        reported_id: reportedId,
        reason,
        details:
          typeof details === "string"
            ? details.slice(0, 1000)
            : null,
        status: "pending"
      }),
      signal: AbortSignal.timeout(8000)
    }
  );

  if (!response.ok) {
    throw new Error("Şikâyet kaydedilemedi.");
  }
}

function removeWaiting(id) {
  waitingUsers.delete(id);
}

function endChat(id, reason = "ended") {
  const session = sessions.get(id);

  if (!session) return;

  clearTimeout(session.timer);

  for (const userId of [session.a, session.b]) {
    sessions.delete(userId);
    io.to(userId).emit("chatEnded", { reason });
  }
}

// Rastgele eşleşme
async function findPartner(socket) {
  if (!socket.connected) return;

  if (matchingUsers.has(socket.id)) return;

  matchingUsers.add(socket.id);
  removeWaiting(socket.id);

  try {
    if (sessions.has(socket.id)) {
      endChat(socket.id, "next");
    }

    const userId = socket.data.userId;

    for (const id of [...waitingUsers]) {
      const partner = io.sockets.sockets.get(id);

      if (!partner || !partner.connected) {
        waitingUsers.delete(id);
        continue;
      }

      if (
        id === socket.id ||
        sessions.has(id) ||
        matchingUsers.has(id)
      ) {
        continue;
      }

      const blocked = await isBlocked(
        userId,
        partner.data.userId
      );

      if (!socket.connected) return;

      if (
        blocked ||
        sessions.has(id) ||
        matchingUsers.has(id) ||
        !waitingUsers.has(id)
      ) {
        continue;
      }

      matchingUsers.add(id);
      waitingUsers.delete(id);

      const session = {
        a: socket.id,
        b: id,
        timer: null
      };

      sessions.set(socket.id, session);
      sessions.set(id, session);

      session.timer = setTimeout(() => {
        endChat(socket.id, "timeout");
      }, CHAT_DURATION);

      socket.emit("matched", {
        peerId: id,
        initiator: true,
        duration: 300
      });

      partner.emit("matched", {
        peerId: socket.id,
        initiator: false,
        duration: 300
      });

      return;
    }

    waitingUsers.add(socket.id);
    socket.emit("waiting");

  } catch (error) {
    console.error("Eşleşme hatası:", error.message);

    socket.emit("statusMessage", {
      message: "Eşleşme sırasında güvenlik kontrolü yapılamadı. Tekrar dene."
    });

  } finally {
    matchingUsers.delete(socket.id);
  }
}

io.on("connection", socket => {
  console.log(
    "Test kullanıcısı bağlandı:",
    socket.data.userId
  );

  socket.on("randomMatch", () => {
    findPartner(socket);
  });

  // Görüntülü görüşme sinyalleri
  socket.on("signal", data => {
    const session = sessions.get(socket.id);

    if (!session || !data) return;

    const partnerId =
      session.a === socket.id
        ? session.b
        : session.a;

    if (data.to !== partnerId) return;

    io.to(partnerId).emit("signal", {
      from: socket.id,
      signal: data.signal
    });
  });

  // Görüşülen kişiyi engelle
  socket.on("blockPartner", async () => {
    const session = sessions.get(socket.id);

    if (!session) {
      socket.emit("actionResult", {
        action: "block",
        success: false,
        message: "Önce biriyle eşleşmelisin."
      });
      return;
    }

    const partnerId =
      session.a === socket.id
        ? session.b
        : session.a;

    const partner =
      io.sockets.sockets.get(partnerId);

    if (!partner) return;

    try {
      await saveBlock(
        socket.data.userId,
        partner.data.userId
      );

      endChat(socket.id, "blocked");

      socket.emit("actionResult", {
        action: "block",
        success: true,
        message: "Kullanıcı engellendi."
      });

    } catch (error) {
      console.error("Engelleme:", error.message);

      socket.emit("actionResult", {
        action: "block",
        success: false,
        message: "Engelleme kaydedilemedi."
      });
    }
  });

  // Görüşülen kişiyi şikâyet et
  socket.on("reportPartner", async data => {
    const session = sessions.get(socket.id);

    if (!session) {
      socket.emit("actionResult", {
        action: "report",
        success: false,
        message: "Önce biriyle eşleşmelisin."
      });
      return;
    }

    const partnerId =
      session.a === socket.id
        ? session.b
        : session.a;

    const partner =
      io.sockets.sockets.get(partnerId);

    if (!partner) return;

    try {
      await saveReport(
        socket.data.userId,
        partner.data.userId,
        data?.reason,
        data?.details
      );

      socket.emit("actionResult", {
        action: "report",
        success: true,
        message: "Şikâyetin alındı. Teşekkür ederiz."
      });

    } catch (error) {
      console.error("Şikâyet:", error.message);

      socket.emit("actionResult", {
        action: "report",
        success: false,
        message: "Şikâyet gönderilemedi."
      });
    }
  });

  // Sonraki kişi
  socket.on("nextPerson", () => {
    endChat(socket.id, "next");
    removeWaiting(socket.id);
    findPartner(socket);
  });

  // Bağlantı kesilince temizle
  socket.on("disconnect", () => {
    removeWaiting(socket.id);
    matchingUsers.delete(socket.id);
    endChat(socket.id, "disconnected");

    console.log(
      "Kullanıcı ayrıldı:",
      socket.data.userId
    );
  });
});

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "VibeChat test server"
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(
    "VibeChat güvenli test sunucusu çalışıyor!"
  );
});
