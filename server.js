
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

const CHAT_DURATION = 5 * 60 * 1000;

const waitingUsers = new Set();
const sessions = new Map();

let matchingBusy = false;

function adminHeaders() {
  if (!SERVICE_KEY) {
    throw new Error("Supabase gizli anahtarı eksik.");
  }

  return {
    apikey: SERVICE_KEY,
    "Content-Type": "application/json"
  };
}

async function supabaseRequest(path, options = {}) {
  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/${path}`,
    {
      ...options,
      headers: {
        ...adminHeaders(),
        ...(options.headers || {})
      },
      signal: AbortSignal.timeout(8000)
    }
  );

  if (!response.ok) {
    console.error(
      "Supabase REST hatası:",
      response.status,
      path.split("?")[0]
    );

    throw new Error(
      `Supabase HTTP ${response.status}`
    );
  }

  return response;
}

// Supabase hesabını doğrula.
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

    if (!user.id || !user.email_confirmed_at) {
      return null;
    }

    return user;
  } catch (error) {
    console.error(
      "Kimlik doğrulama hatası:",
      error.message
    );

    return null;
  }
}

// Doğrulanmış kullanıcılar bağlanabilir.
io.use(async (socket, next) => {
  const token = socket.handshake.auth?.token;
  const user = await verifyUser(token);

  if (!user) {
    return next(
      new Error(
        "Giriş yapmalı ve e-postanı doğrulamalısın."
      )
    );
  }

  const alreadyConnected =
    [...io.sockets.sockets.values()].some(
      s => s.data.userId === user.id
    );

  if (alreadyConnected) {
    return next(
      new Error("Bu hesap zaten bağlı.")
    );
  }

  socket.data.userId = user.id;
  next();
});

// Kullanıcılar birbirini engellemiş mi?
async function isBlocked(userA, userB) {
  const params = new URLSearchParams();

  params.set("select", "id");
  params.set(
    "or",
    `(and(blocker_id.eq.${userA},blocked_id.eq.${userB}),and(blocker_id.eq.${userB},blocked_id.eq.${userA}))`
  );

  params.set("limit", "1");

  const response = await supabaseRequest(
    `user_blocks?${params.toString()}`
  );

  const rows = await response.json();

  return rows.length > 0;
}

// Kullanıcı engelleme.
async function saveBlock(blockerId, blockedId) {
  await supabaseRequest(
    "user_blocks?on_conflict=blocker_id,blocked_id",
    {
      method: "POST",
      headers: {
        Prefer:
          "resolution=ignore-duplicates,return=minimal"
      },
      body: JSON.stringify({
        blocker_id: blockerId,
        blocked_id: blockedId
      })
    }
  );
}

// Kullanıcı şikâyeti.
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

  await supabaseRequest("user_reports", {
    method: "POST",
    headers: {
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
    })
  });
}

function getPartnerSocket(socket) {
  const session = sessions.get(socket.id);

  if (!session) return null;

  const partnerId =
    session.a === socket.id
      ? session.b
      : session.a;

  return io.sockets.sockets.get(partnerId) || null;
}

function endChat(socketId, reason = "ended") {
  const session = sessions.get(socketId);

  if (!session) return;

  clearTimeout(session.timer);

  sessions.delete(session.a);
  sessions.delete(session.b);

  io.to(session.a).emit("chatEnded", { reason });
  io.to(session.b).emit("chatEnded", { reason });
}

function startChat(socketA, socketB) {
  waitingUsers.delete(socketA.id);
  waitingUsers.delete(socketB.id);

  const session = {
    a: socketA.id,
    b: socketB.id,
    timer: null
  };

  sessions.set(socketA.id, session);
  sessions.set(socketB.id, session);

  session.timer = setTimeout(() => {
    endChat(socketA.id, "timeout");
  }, CHAT_DURATION);

  socketA.emit("matched", {
    peerId: socketB.id,
    initiator: true,
    duration: 300
  });

  socketB.emit("matched", {
    peerId: socketA.id,
    initiator: false,
    duration: 300
  });

  console.log("İki kullanıcı eşleşti.");
}

// Rastgele eşleştirme.
async function processMatching() {
  if (matchingBusy) return;

  matchingBusy = true;

  try {
    let matched;

    do {
      matched = false;

      const ids = [...waitingUsers];

      for (let i = 0; i < ids.length; i++) {
        const first =
          io.sockets.sockets.get(ids[i]);

        if (
          !first?.connected ||
          sessions.has(ids[i]) ||
          !waitingUsers.has(ids[i])
        ) {
          waitingUsers.delete(ids[i]);
          continue;
        }

        for (let j = i + 1; j < ids.length; j++) {
          const second =
            io.sockets.sockets.get(ids[j]);

          if (
            !second?.connected ||
            sessions.has(ids[j]) ||
            !waitingUsers.has(ids[j])
          ) {
            waitingUsers.delete(ids[j]);
            continue;
          }

          if (
            first.data.userId === second.data.userId
          ) {
            continue;
          }

          const blocked = await isBlocked(
            first.data.userId,
            second.data.userId
          );

          if (blocked) continue;

          if (
            !first.connected ||
            !second.connected ||
            !waitingUsers.has(first.id) ||
            !waitingUsers.has(second.id) ||
            sessions.has(first.id) ||
            sessions.has(second.id)
          ) {
            continue;
          }

          startChat(first, second);
          matched = true;
          break;
        }

        if (matched) break;
      }
    } while (matched);

  } catch (error) {
    console.error(
      "Eşleşme hatası:",
      error.message
    );

    for (const id of waitingUsers) {
      io.to(id).emit("statusMessage", {
        message:
          "Eşleşme kontrolü başarısız. Tekrar Kişi Bul'a bas."
      });
    }

    waitingUsers.clear();

  } finally {
    matchingBusy = false;
  }
}

function findPartner(socket) {
  if (!socket.connected) return;

  if (sessions.has(socket.id)) {
    endChat(socket.id, "next");
  }

  waitingUsers.add(socket.id);

  socket.emit("waiting");

  processMatching();
}

io.on("connection", socket => {
  console.log(
    "Kullanıcı bağlandı:",
    socket.data.userId
  );

  socket.on("randomMatch", () => {
    findPartner(socket);
  });

  // Görüntü ve ses sinyalleri.
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

  // Engelle.
  socket.on("blockPartner", async () => {
    const session = sessions.get(socket.id);
    const partner = getPartnerSocket(socket);

    if (!session || !partner) {
      socket.emit("actionResult", {
        action: "block",
        success: false,
        message: "Önce biriyle eşleşmelisin."
      });
      return;
    }

    try {
      await saveBlock(
        socket.data.userId,
        partner.data.userId
      );

      if (sessions.get(socket.id) === session) {
        endChat(socket.id, "blocked");
      }

      socket.emit("actionResult", {
        action: "block",
        success: true,
        message: "Kullanıcı engellendi."
      });

    } catch (error) {
      console.error(
        "Engelleme hatası:",
        error.message
      );

      socket.emit("actionResult", {
        action: "block",
        success: false,
        message: "Engelleme kaydedilemedi."
      });
    }
  });

  // Şikâyet et.
  socket.on("reportPartner", async data => {
    const partner = getPartnerSocket(socket);

    if (!partner) {
      socket.emit("actionResult", {
        action: "report",
        success: false,
        message: "Önce biriyle eşleşmelisin."
      });
      return;
    }

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
        message: "Şikâyetin alındı."
      });

    } catch (error) {
      console.error(
        "Şikâyet hatası:",
        error.message
      );

      socket.emit("actionResult", {
        action: "report",
        success: false,
        message: "Şikâyet gönderilemedi."
      });
    }
  });

  socket.on("nextPerson", () => {
    waitingUsers.delete(socket.id);
    endChat(socket.id, "next");
    findPartner(socket);
  });

  socket.on("disconnect", () => {
    waitingUsers.delete(socket.id);
    endChat(socket.id, "disconnected");

    console.log(
      "Kullanıcı ayrıldı:",
      socket.data.userId
    );
  });
});

// Sunucu sağlık kontrolü.
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "VibeChat server"
  });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(
    "VibeChat güvenli test sunucusu çalışıyor!"
  );
});
