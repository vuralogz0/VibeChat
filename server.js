
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

app.get("/login.html", (req, res) => {
  res.sendFile("login.html", { root: "public" });
});

app.get("/signup.html", (req, res) => {
  res.sendFile("signup.html", { root: "public" });
});

app.get("/profile.html", (req, res) => {
  res.sendFile("profile.html", { root: "public" });
});

// Supabase proje adresi
const SUPABASE_URL =
  "https://orznejzevoclmjqfulzh.supabase.co";

// Render Environment ayarlarından okunur
const TEST_USERS = new Set(
  [
    process.env.TEST_USER_1,
    process.env.TEST_USER_2
  ].filter(Boolean)
);

const CHAT_DURATION = 5 * 60 * 1000;

const waitingUsers = new Set();
const sessions = new Map();
const recentPartners = new Map();

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
          apikey: process.env.SUPABASE_PUBLISHABLE_KEY || ""
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
    console.error("Kimlik doğrulama hatası:", error.message);
    return null;
  }
}

// Her Socket.IO bağlantısını doğrula
io.use(async (socket, next) => {
  const token = socket.handshake.auth?.token;

  const user = await verifyUser(token);

  if (!user) {
    return next(
      new Error("Bu hesap test için yetkili değil.")
    );
  }

  // Aynı hesapla iki farklı bağlantıyı engelle
  const alreadyConnected = [...io.sockets.sockets.values()]
    .some(s => s.data.userId === user.id);

  if (alreadyConnected) {
    return next(
      new Error("Bu hesap zaten bağlı.")
    );
  }

  socket.data.userId = user.id;
  next();
});

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

function findPartner(socket) {
  removeWaiting(socket.id);

  if (sessions.has(socket.id)) {
    endChat(socket.id, "next");
  }

  for (const id of waitingUsers) {
    const partner = io.sockets.sockets.get(id);

    if (!partner) {
      waitingUsers.delete(id);
      continue;
    }

    if (id === socket.id || sessions.has(id)) {
      continue;
    }

    if (
      recentPartners.get(socket.id) === id ||
      recentPartners.get(id) === socket.id
    ) {
      continue;
    }

    waitingUsers.delete(id);

    const session = {
      a: socket.id,
      b: id,
      timer: null
    };

    sessions.set(socket.id, session);
    sessions.set(id, session);

    recentPartners.set(socket.id, id);
    recentPartners.set(id, socket.id);

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
}

io.on("connection", socket => {
  console.log("Test kullanıcısı bağlandı:", socket.id);

  socket.on("randomMatch", () => {
    findPartner(socket);
  });

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

  socket.on("nextPerson", () => {
    endChat(socket.id, "next");
    removeWaiting(socket.id);
    findPartner(socket);
  });

  socket.on("disconnect", () => {
    removeWaiting(socket.id);
    endChat(socket.id, "disconnected");
    recentPartners.delete(socket.id);

    console.log("Kullanıcı ayrıldı:", socket.id);
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
  console.log("VibeChat test sunucusu çalışıyor!");
});
