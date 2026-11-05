const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

let waitingUser = null;

io.on("connection", (socket) => {
    console.log("Kullanıcı bağlandı:", socket.id);

    socket.on("randomMatch", () => {
        if (waitingUser && waitingUser !== socket.id) {
            const partner = io.sockets.sockets.get(waitingUser);

            if (partner) {
                socket.emit("matched", {
                    peerId: partner.id,
                    initiator: true
                });

                partner.emit("matched", {
                    peerId: socket.id,
                    initiator: false
                });

                waitingUser = null;
                return;
            }
        }

        waitingUser = socket.id;
        socket.emit("waiting");
    });

    socket.on("signal", (data) => {
        if (!data || typeof data.to !== "string") return;

        const partner = io.sockets.sockets.get(data.to);

        if (partner) {
            partner.emit("signal", {
                from: socket.id,
                signal: data.signal
            });
        }
    });

    socket.on("disconnect", () => {
        if (waitingUser === socket.id) {
            waitingUser = null;
        }

        console.log("Kullanıcı ayrıldı:", socket.id);
    });
});

const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
    console.log("VibeChat sunucusu çalışıyor!");
});