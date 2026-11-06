
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

const CHAT_DURATION = 5 * 60 * 1000;
const waitingUsers = new Set();
const sessions = new Map();
const recentPartners = new Map();

function removeWaiting(id) {
    waitingUsers.delete(id);
}

function endChat(id, reason = "ended") {
    const session = sessions.get(id);
    if (!session) return;

    clearTimeout(session.timer);

    const users = [session.a, session.b];

    for (const userId of users) {
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

io.on("connection", (socket) => {
    console.log("Kullanıcı bağlandı:", socket.id);

    socket.on("randomMatch", () => {
        // Yaş doğrulaması tamamlanana kadar
        // gerçek kullanıcı eşleşmeleri kapalı.
        socket.emit("statusMessage", {
            message: "VibeChat şu anda test aşamasında."
        });
    });

    socket.on("signal", (data) => {
        const session = sessions.get(socket.id);

        if (!session || !data) return;

        const partnerId =
            session.a === socket.id ? session.b : session.a;

        if (data.to !== partnerId) return;

        io.to(partnerId).emit("signal", {
            from: socket.id,
            signal: data.signal
        });
    });

    socket.on("nextPerson", () => {
        endChat(socket.id, "next");
        removeWaiting(socket.id);
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
    console.log("VibeChat sunucusu çalışıyor!");
});
