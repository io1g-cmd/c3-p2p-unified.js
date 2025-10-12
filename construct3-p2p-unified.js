/**
 * Construct 3 P2P 客戶端庫 - 整合版本
 * @version1 基於 Construct 3 點對點通道服務的 JavaScript 工具庫
 * 
 * @author AI Assistant
 * @version 2.0.0
 */

class EventManager {
    constructor() {
        this.events = new Map();
    }

    /**
     * 註冊事件監聽器
     * @param {string} event - 事件名稱
     * @param {Function} callback - 回調函數
     */
    on(event, callback) {
        if (!this.events.has(event)) {
            this.events.set(event, []);
        }
        this.events.get(event)?.push(callback);
    }

    /**
     * 觸發事件
     * @param {string} event - 事件名稱
     * @param {...any} args - 事件參數
     */
    emit(event, ...args) {
        const callbacks = this.events.get(event);
        if (callbacks) {
            callbacks.forEach((callback) => callback(...args));
        }
    }

    /**
     * 移除事件監聽器
     * @param {string} event - 事件名稱
     * @param {Function} callback - 要移除的回調函數
     */
    off(event, callback) {
        const callbacks = this.events.get(event);
        if (callbacks) {
            this.events.set(event, callbacks.filter((cb) => cb !== callback));
        }
    }

    /**
     * 移除所有事件監聽器
     */
    removeAllListeners() {
        this.events.clear();
    }
}

class ChannelSendQueue {
    constructor(datachannel, peerId, tag) {
        this.datachannel = datachannel;
        this.peerId = peerId;
        this.tag = tag;
        this.queue = [];
        this.sending = false;
    }

    /**
     * 將消息加入發送隊列
     * @param {string} message - 要發送的消息
     * @param {number} delay - 延遲時間（毫秒）
     */
    enqueue(message, delay) {
        const entry = { message, ready: false };
        this.queue.push(entry);
        setTimeout(() => {
            entry.ready = true;
            this.processNext();
        }, delay);
        this.processNext();
    }

    /**
     * 處理隊列中的下一條消息
     */
    processNext() {
        if (this.sending || this.queue.length === 0) return;
        
        const entry = this.queue[0];
        if (!entry.ready) return;
        
        this.sending = true;
        try {
            this.datachannel.send(entry.message);
        } catch (e) {
            console.error(`[${this.tag}] 發送消息到 ${this.peerId} 時出錯:`, e);
        }
        
        this.queue.shift();
        this.sending = false;
        this.processNext();
    }
}

class P2PClient {
    constructor(tag, eventManager) {
        this.tag = tag;
        this.eventManager = eventManager;
        
        // WebSocket 連接
        this.ws = null;
        this.SUBPROTOCOL = "c2multiplayer";
        
        // 連接狀態
        this.isLoggedIn = false;
        this.isConnected = false;
        this.isHost = false;
        
        // 用戶信息
        this.myid = "";
        this.myAlias = "";
        this.hostId = "";
        this.hostAlias = "";
        
        // 房間信息 (使用新的命名)
        this.traceID = "";      // 原 game - 追蹤 ID
        this.publicKey = "";    // 原 instance - 公鑰
        this.idSign = "";       // 原 room - Peer ID 的簽名哈希 (連接信令服務獲得 Peer ID → 簽署 → 得到 Hash)
        this.isOnRoom = false;
        
        // WebRTC 連接
        this.connectionsWebRTC = new Map();
        this.ice_servers = [];
        
        // 模擬網絡條件
        this.simLatency = 0;
        this.simPdv = 0;
        this.simPacketLoss = 0;
        
        // 發送隊列
        this.sendQueues = new Map();
        
        // 其他狀態
        this.leaveReason = "";
        this.peersList = []; // Array of tuples [peerId, peerAlias]
        this.peerCount = 0;
    }

    /**
     * 連接到信令服務器
     * @param {string} serverUrl - 服務器 URL
     */
    async connectToSignallingServer(serverUrl) {
        if (this.ws) {
            console.warn(`[${this.tag}] 已經連接到信令服務器`);
            return;
        }

        try {
            this.ws = new WebSocket(serverUrl, this.SUBPROTOCOL);
            
            this.ws.onopen = () => {
                console.log(`[${this.tag}] WebSocket 連接已建立`);
            };
            
            this.ws.onmessage = (event) => {
                try {
                    const msg = JSON.parse(event.data);
                    this.signallingServerMessageHandler(msg);
                } catch (e) {
                    console.error(`[${this.tag}] 解析信令服務器消息失敗:`, e);
                }
            };
            
            this.ws.onerror = (error) => {
                console.error(`[${this.tag}] WebSocket 錯誤:`, error);
                this.eventManager.emit("onError", {
                    clientTag: this.tag,
                    errorMessage: `連接到信令服務器失敗 (${this.tag})`,
                });
            };
            
            this.ws.onclose = (event) => {
                console.log(`[${this.tag}] WebSocket 連接已關閉`, event);
                this.isConnected = false;
                this.isLoggedIn = false;
                this.ws = null;
                this.eventManager.emit("disconnected", {
                    clientTag: this.tag,
                    code: event.code,
                    reason: event.reason,
                });
            };
        } catch (error) {
            console.error(`[${this.tag}] 連接信令服務器時出錯:`, error);
            this.eventManager.emit("onError", {
                clientTag: this.tag,
                errorMessage: `連接信令服務器時出錯: ${error.message}`,
            });
        }
    }

    /**
     * 處理信令服務器消息
     * @param {Object} msg - 消息對象
     */
    signallingServerMessageHandler(msg) {
        switch (msg.message) {
            case "welcome":
                this.myid = msg.clientid;
                let rawIceServers = msg.ice_servers || [];
                this.ice_servers = rawIceServers.map((server) => {
                    if (typeof server === "string") {
                        return { urls: server };
                    }
                    return server;
                });
                
                this.eventManager.emit("connected", {
                    clientTag: this.tag,
                    clientId: this.myid,
                });
                this.isConnected = true;
                break;

            case "login-ok":
                this.isLoggedIn = true;
                this.myAlias = msg.alias;
                this.eventManager.emit("loggedIn", {
                    clientTag: this.tag,
                    alias: msg.alias,
                    clientId: this.myid,
                });
                break;

            case "join-ok":
                this.isHost = msg.host;
                this.hostId = msg.hostid;
                this.hostAlias = msg.hostalias;
                this.isOnRoom = true;
                this.traceID = msg.game || this.traceID;
                this.publicKey = msg.instance || this.publicKey;
                this.idSign = msg.room || this.idSign;
                
                if (this.isHost) {
                    this.hostId = this.myid;
                    this.hostAlias = this.myAlias;
                }
                
                this.eventManager.emit("joinedRoom", {
                    clientTag: this.tag,
                    isHost: this.isHost,
                    hostId: this.hostId,
                    hostAlias: this.hostAlias,
                });
                break;

            case "peer-joined":
                this.onPeerJoinedSGWS(msg.peerid, msg.peeralias);
                break;

            case "offer":
                this.onPeerJoinedSGWS(msg.from, this.hostAlias);
                this.handleOffer(msg.from, msg.offer);
                break;

            case "answer":
                this.handleAnswer(msg.from, msg.answer);
                break;

            case "icecandidate":
                this.handleIceCandidate(msg.from, msg.icecandidate);
                break;

            case "kicked":
                if (msg.reason === "host-left") {
                    this.disconnectFromSignalling();
                } else {
                    this.eventManager.emit("onKicked", {
                        clientTag: this.tag,
                        reason: msg.reason,
                    });
                    this.disconnectFromRoom();
                }
                break;

            case "leave-ok":
                this.eventManager.emit("leftRoom", {
                    clientTag: this.tag,
                });
                break;

            case "error":
                this.eventManager.emit("onError", {
                    clientTag: this.tag,
                    errorMessage: msg.details,
                });
                break;

            case "room-list":
                this.eventManager.emit("room-list", {
                    clientTag: this.tag,
                    roomListData: msg.list,
                });
                break;

            case "instance-list":
                this.eventManager.emit("instance-list", {
                    clientTag: this.tag,
                    instanceListData: msg.list,
                });
                break;
        }
    }

    /**
     * 登錄到信令服務器
     * @param {string} alias - 用戶別名
     */
    async loginToSignallingServer(alias) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            throw new Error("WebSocket 未連接");
        }
        
        this.ws.send(JSON.stringify({
            message: "login",
            protocolrev: 1,
            datachannelrev: 2,
            compressionformats: ["deflate", "gzip"],
            alias,
        }));
    }

    /**
     * 自動加入房間
     * @param {string} traceID - Trace ID of a person/object
     * @param {string} publicKey - Public key
     * @param {string} idSign - Hash of the ID from server (Peer ID 的簽名哈希)
     * @param {number} max_clients - 最大客戶端數量
     * @param {boolean} lock_when_full - 滿員時是否鎖定
     */
    async autoJoinRoom(traceID, publicKey, idSign, max_clients, lock_when_full) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            throw new Error("WebSocket 未連接");
        }
        
        this.sendSgws({
            message: "auto-join",
            game: traceID,
            instance: publicKey,
            room: idSign,
            max_clients,
            lock_when_full,
        });
    }

    /**
     * 加入房間
     * @param {string} traceID - 追蹤 ID
     * @param {string} publicKey - 公鑰
     * @param {string} idSign - ID 簽名 (Peer ID 的簽名哈希)
     * @param {number} max_clients - 最大客戶端數量
     */
    async joinRoom(traceID, publicKey, idSign, max_clients) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            throw new Error("WebSocket 未連接");
        }
        
        this.traceID = traceID;
        this.publicKey = publicKey;
        this.idSign = idSign;
        
        this.sendSgws({
            message: "join",
            game: traceID,
            instance: publicKey,
            room: idSign,
            max_clients,
        });
    }

    /**
     * 處理對等節點加入
     * @param {string} peerId - 對等節點 ID
     * @param {string} peerAlias - 對等節點別名
     */
    onPeerJoinedSGWS(peerId, peerAlias) {
        const peerConnection = {
            conn: new RTCPeerConnection({ iceServers: this.ice_servers || [] }),
            channels: {
                unorderedReliable: null,
                orderedReliable: null,
                unreliable: null,
            },
            state: "new",
            lastPing: null,
            isReady: false,
            peerId,
            peerAlias,
        };
        
        this.connectionsWebRTC.set(peerId, peerConnection);
        
        // 設置 ICE 候選處理
        peerConnection.conn.onicecandidate = (e) => {
            if (e.candidate) {
                this.sendSgws({
                    message: "icecandidate",
                    toclientid: peerId,
                    icecandidate: e.candidate,
                });
            }
        };
        
        // 設置統一的連接狀態監聽
        this.setupConnectionStateListeners(peerConnection, peerId, peerAlias);
        
        if (this.isHost) {
            this.setupDataChannel(peerConnection);
        } else {
            // 客戶端處理數據通道
            let channelsReady = 0;
            const expectedChannels = 3;
            
            peerConnection.conn.ondatachannel = (event) => {
                const dc = event.channel;
                
                // 設置數據通道
                if (dc.label === "ordered-reliable") {
                    peerConnection.channels.orderedReliable = dc;
                } else if (dc.label === "unordered-reliable") {
                    peerConnection.channels.unorderedReliable = dc;
                } else if (dc.label === "unreliable") {
                    peerConnection.channels.unreliable = dc;
                } else {
                    console.warn(`[${this.tag}] 未知通道: ${dc.label}`);
                    return;
                }
                
                dc.onmessage = (e) => {
                    this.onPeerMessageReceived(peerConnection.peerId, e.data, peerConnection.peerAlias);
                };
                
                dc.onopen = () => {
                    channelsReady++;
                    if (channelsReady === expectedChannels) {
                        peerConnection.isReady = true;
                        this.sendQueues.set(peerId, new ChannelSendQueue(peerConnection.channels.orderedReliable, peerId, this.tag));
                        // 不重複觸發 joinedRoom 事件，只在信令服務器確認時觸發
                    }
                };
                
                dc.onclose = () => {
                    if (this.connectionsWebRTC.has(peerId)) {
                        this.eventManager.emit("onPeerDisconnected", {
                            clientTag: this.tag,
                            peerId: peerId,
                            peerAlias: peerAlias,
                        });
                        this.removePeerConnection(peerId, { emit: false });
                    }
                };
            };
        }
        
        // 主機創建 offer
        if (this.isHost) {
            this.sendQueues.set(peerId, new ChannelSendQueue(peerConnection.channels.orderedReliable, peerId, this.tag));
            
            peerConnection.conn.createOffer().then(async (offer) => {
                return peerConnection.conn.setLocalDescription(offer).then(() => {
                    this.sendSgws({
                        message: "offer",
                        offer,
                        toclientid: peerConnection.peerId,
                    });
                });
            });
            
            this.waitForReady(peerConnection).then(() => {
                peerConnection.isReady = true;
                this.eventManager.emit("peerJoined", {
                    peerId: peerConnection.peerId,
                    clientTag: this.tag,
                    peerAlias: peerConnection.peerAlias,
                });
                this.sendPeerConnecteds(peerConnection.peerId);
                this.broadcastPeerConnected(peerConnection.peerId, peerConnection.peerAlias);
                this.sendSgws({
                    message: "confirm-peer",
                    id: peerConnection.peerId,
                });
            });
        }
    }

    /**
     * 設置統一的連接狀態監聽
     * @param {Object} peerConnection - 對等連接對象
     * @param {string} peerId - 對等節點 ID
     * @param {string} peerAlias - 對等節點別名
     */
    setupConnectionStateListeners(peerConnection, peerId, peerAlias) {
        const handleConnectionStateChange = () => {
            const state = peerConnection.conn.connectionState;
            if (state === "disconnected" || state === "failed" || state === "closed") {
                if (this.isHost) {
                    this.removePeerConnection(peerId);
                } else {
                    this.eventManager.emit("onPeerDisconnected", {
                        clientTag: this.tag,
                        peerId: peerId,
                        peerAlias: peerAlias,
                    });
                    this.removePeerConnection(peerId, { emit: false });
                }
            }
        };
        
        peerConnection.conn.onconnectionstatechange = handleConnectionStateChange;
        peerConnection.conn.oniceconnectionstatechange = handleConnectionStateChange;
    }

    /**
     * 設置數據通道
     * @param {Object} peerConnection - 對等連接對象
     */
    setupDataChannel(peerConnection) {
        const dc_protocol = "C3M_" + this.traceID + "_" + this.publicKey + "_" + this.idSign;
        
        const assignOnMessage = (dc) => {
            if (!dc) return;
            
            dc.onmessage = (e) => {
                this.onPeerMessageReceived(peerConnection.peerId, e.data, peerConnection.peerAlias);
            };
            
            dc.onclose = () => {
                this.removePeerConnection(peerConnection.peerId);
            };
        };
        
        // 創建三種類型的數據通道
        peerConnection.channels.orderedReliable = peerConnection.conn.createDataChannel("ordered-reliable", {
            ordered: true,
            protocol: dc_protocol,
        });
        assignOnMessage(peerConnection.channels.orderedReliable);
        
        peerConnection.channels.unorderedReliable = peerConnection.conn.createDataChannel("unordered-reliable", {
            ordered: false,
            protocol: dc_protocol,
        });
        assignOnMessage(peerConnection.channels.unorderedReliable);
        
        peerConnection.channels.unreliable = peerConnection.conn.createDataChannel("unreliable", {
            ordered: false,
            maxRetransmits: 0,
            protocol: dc_protocol,
        });
        assignOnMessage(peerConnection.channels.unreliable);
    }

    /**
     * 處理 offer
     * @param {string} peerId - 對等節點 ID
     * @param {Object} offer - RTC offer
     */
    async handleOffer(peerId, offer) {
        const peerConnection = this.connectionsWebRTC.get(peerId);
        if (!peerConnection) {
            console.warn(`[${this.tag}] 找不到對等連接 ${peerId}`);
            return;
        }
        
        await peerConnection.conn.setRemoteDescription(new RTCSessionDescription(offer));
        const answer = await peerConnection.conn.createAnswer();
        await peerConnection.conn.setLocalDescription(answer);
        
        this.sendSgws({
            message: "answer",
            answer,
            toclientid: peerId,
        });
    }

    /**
     * 處理 answer
     * @param {string} peerId - 對等節點 ID
     * @param {Object} answer - RTC answer
     */
    async handleAnswer(peerId, answer) {
        const peerConnection = this.connectionsWebRTC.get(peerId);
        if (!peerConnection) {
            return;
        }
        
        await peerConnection.conn.setRemoteDescription(new RTCSessionDescription(answer));
    }

    /**
     * 處理 ICE 候選
     * @param {string} peerId - 對等節點 ID
     * @param {Object} candidate - ICE 候選
     */
    async handleIceCandidate(peerId, candidate) {
        const peerConnection = this.connectionsWebRTC.get(peerId);
        if (!peerConnection) {
            return;
        }
        
        await peerConnection.conn.addIceCandidate(new RTCIceCandidate(candidate));
    }

    /**
     * 發送消息到信令服務器
     * @param {Object} message - 消息對象
     */
    async sendSgws(message) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            throw new Error("WebSocket 未連接");
        }
        
        this.ws.send(JSON.stringify(message));
    }

    /**
     * 等待連接就緒
     * @param {Object} peerConnection - 對等連接對象
     * @returns {Promise} 就緒 Promise
     */
    waitForReady(peerConnection) {
        return new Promise((resolve) => {
            const checkReady = () => {
                const connReady = peerConnection.conn.connectionState === "connected";
                const allChannelsReady = Object.values(peerConnection.channels).every((dc) => dc && dc.readyState === "open");
                
                if (connReady && allChannelsReady) {
                    resolve();
                }
            };
            
            peerConnection.conn.onconnectionstatechange = checkReady;
            for (const dc of Object.values(peerConnection.channels)) {
                if (dc) dc.onopen = checkReady;
            }
        });
    }

    /**
     * 發送消息到對等節點
     * @param {string} peerId - 對等節點 ID
     * @param {string} message - 消息內容
     * @param {string} channel - 通道類型
     */
    sendMessageToPeer(peerId, message, channel) {
        if (peerId === "" && this.isHost) return;
        else if (peerId === "" && !this.isHost) peerId = this.hostId;
        
        const peerConnection = this.connectionsWebRTC.get(peerId);
        if (!peerConnection) return;
        
        const datachannel = peerConnection.channels[channel];
        if (!datachannel || datachannel.readyState !== "open") return;
        
        // 模擬網絡條件
        if (channel === "unreliable" && this.simPacketLoss > 0 && Math.random() < this.simPacketLoss / 100) {
            return;
        }
        
        let delayMultiplier = 1;
        if (channel !== "unreliable" && this.simPacketLoss > 0 && Math.random() < this.simPacketLoss / 100) {
            delayMultiplier = 3;
        }
        
        const jitter = Math.random() * this.simPdv * 2 - this.simPdv;
        const delay = Math.max(0, (this.simLatency + jitter) * delayMultiplier);
        
        if (channel === "orderedReliable") {
            const queueMap = this.sendQueues.get(peerId);
            if (queueMap) {
                queueMap.enqueue(message, delay);
                return;
            }
        }
        
        setTimeout(() => {
            try {
                datachannel.send(message);
            } catch (e) {
                console.error(`[${this.tag}] 發送消息到 ${peerId} 時出錯:`, e);
            }
        }, delay);
    }

    /**
     * 廣播消息到所有對等節點
     * @param {string} fromId - 發送者 ID
     * @param {string} message - 消息內容
     * @param {string} channel - 通道類型
     */
    broadcastMessageToPeers(fromId, message, channel) {
        if (!this.isHost) return;
        
        for (const [peerId, _] of this.connectionsWebRTC.entries()) {
            if (peerId === fromId) continue;
            this.sendMessageToPeer(peerId, message, channel);
        }
    }

    /**
     * 處理對等節點消息
     * @param {string} peerId - 對等節點 ID
     * @param {string} message - 消息內容
     * @param {string} peerAlias - 對等節點別名
     */
    onPeerMessageReceived = (peerId, message, peerAlias) => {
        try {
            const parsedMessage = JSON.parse(message);
            const senderId = parsedMessage.fromId || peerId;
            
            switch (parsedMessage.type) {
                case "default":
                    this.eventManager.emit("peerMessage", {
                        peerId: senderId,
                        message: parsedMessage.message,
                        clientTag: this.tag,
                        peerAlias,
                        tag: parsedMessage.tag,
                    });
                    break;
                    
                case "chat":
                    this.eventManager.emit("peerMessage", {
                        peerId: senderId,
                        message: parsedMessage.message,
                        clientTag: this.tag,
                        peerAlias,
                        tag: parsedMessage.tag,
                    });
                    break;
                    
                case "gameData":
                    this.eventManager.emit("peerMessage", {
                        peerId: senderId,
                        message: JSON.stringify(parsedMessage.data),
                        clientTag: this.tag,
                        peerAlias,
                        tag: parsedMessage.tag,
                    });
                    break;
                    
                case "peer-connected":
                    this.eventManager.emit("peerJoined", {
                        peerId: parsedMessage.peerId,
                        clientTag: this.tag,
                        peerAlias: parsedMessage.peerAlias,
                    });
                    break;
                    
                case "peer-connecteds-list":
                    for (const p of parsedMessage.peers) {
                        if (p.peerId === this.myid) continue;
                        this.eventManager.emit("peerJoined", {
                            peerId: p.peerId,
                            clientTag: this.tag,
                            peerAlias: p.alias,
                        });
                    }
                    break;
                    
                case "kick":
                    if (!this.isHost) {
                        this.leaveReason = parsedMessage.reason || "";
                        this.disconnectFromRoom();
                        this.eventManager.emit("onKicked", {
                            clientTag: this.tag,
                            reason: this.leaveReason,
                        });
                    }
                    break;
            }
        } catch (e) {
            console.error(`[${this.tag}] 解析對等節點消息失敗:`, e);
        }
    };

    /**
     * 廣播對等節點連接事件
     * @param {string} peerId - 對等節點 ID
     * @param {string} peerAlias - 對等節點別名
     */
    broadcastPeerConnected(peerId, peerAlias) {
        const message = JSON.stringify({
            type: "peer-connected",
            peerId,
            peerAlias,
        });
        this.broadcastMessageToPeers(peerId, message, "orderedReliable");
    }

    /**
     * 發送對等節點連接列表
     * @param {string} peerId - 對等節點 ID
     */
    sendPeerConnecteds(peerId) {
        const peers = [];
        peers.push({ peerId: this.hostId, alias: this.hostAlias });
        
        for (const [id, conn] of this.connectionsWebRTC.entries()) {
            peers.push({ peerId: id, alias: conn.peerAlias });
        }
        
        const message = JSON.stringify({
            type: "peer-connecteds-list",
            peers,
        });
        
        this.sendMessageToPeer(peerId, message, "orderedReliable");
    }

    /**
     * 踢出對等節點
     * @param {string} peerId - 對等節點 ID
     * @param {string} reason - 踢出原因
     */
    kickPeer(peerId, reason) {
        const peerConnection = this.connectionsWebRTC.get(peerId);
        if (!this.isHost && !this.isOnRoom && !peerConnection) return;
        
        this.sendMessageToPeer(peerId, JSON.stringify({
            type: "kick",
            reason,
        }), "unorderedReliable");
    }

    /**
     * 請求房間列表
     * @param {string} traceID - 追蹤 ID
     * @param {string} publicKey - 公鑰
     * @param {string} which - 查詢類型
     */
    requestRoomList(traceID, publicKey, which) {
        this.sendSgws({
            message: "list-rooms",
            game: traceID,
            instance: publicKey,
            which,
        });
    }

    /**
     * 請求實例列表
     * @param {string} traceID - 追蹤 ID
     */
    requestInstanceList(traceID) {
        this.sendSgws({
            message: "list-instances",
            game: traceID,
        });
    }

    /**
     * 設置網絡模擬參數
     * @param {number} latency - 延遲（毫秒）
     * @param {number} pdv - 延遲變化（毫秒）
     * @param {number} loss - 丟包率（百分比）
     */
    setSimulationParams(latency, pdv, loss) {
        this.simLatency = latency;
        this.simPdv = pdv;
        this.simPacketLoss = loss;
    }

    /**
     * 從信令服務器斷開連接
     */
    disconnectFromSignalling = () => {
        if (this.ws) {
            this.isConnected = false;
            this.isLoggedIn = false;
            this.ws.close();
        }
    };

    /**
     * 從房間斷開連接
     */
    disconnectFromRoom() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.sendSgws({
                message: "leave",
            });
        }
        
        for (const peerId of this.connectionsWebRTC.keys()) {
            this.removePeerConnection(peerId, { emit: this.isHost });
        }
        
        if (!this.isHost) {
            this.eventManager.emit("onPeerDisconnected", {
                clientTag: this.tag,
                peerId: this.myid,
                peerAlias: this.myAlias,
            });
        }
        
        this.isOnRoom = false;
        this.idSign = "";
        this.hostId = "";
        this.hostAlias = "";
    }

    /**
     * 移除對等連接
     * @param {string} peerId - 對等節點 ID
     * @param {Object} options - 選項
     */
    removePeerConnection(peerId, options = { emit: true }) {
        const peerConnection = this.connectionsWebRTC.get(peerId);
        if (!peerConnection) return;
        
        for (const channel of Object.values(peerConnection.channels)) {
            if (channel) {
                channel.close();
            }
        }
        
        peerConnection.conn.close();
        this.connectionsWebRTC.delete(peerId);
        this.sendQueues.delete(peerId);
        
        if (options.emit && peerConnection.isReady) {
            this.eventManager.emit("onPeerDisconnected", {
                clientTag: this.tag,
                peerId,
                peerAlias: peerConnection.peerAlias,
            });
        }
    }

    /**
     * 在信令服務器上離開房間
     */
    leaveRoomOnSignalling() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.sendSgws({
                message: "leave",
            });
        }
    }

    /**
     * 獲取可序列化的狀態
     * @returns {Object} 狀態對象
     */
    toSerializable() {
        return {
            tag: this.tag,
            isLoggedIn: this.isLoggedIn,
            isConnected: this.isConnected,
            isHost: this.isHost,
            myid: this.myid,
            myAlias: this.myAlias,
            hostId: this.hostId,
            hostAlias: this.hostAlias,
            traceID: this.traceID,
            publicKey: this.publicKey,
            idSign: this.idSign,
            isOnRoom: this.isOnRoom,
            ice_servers: this.ice_servers.map((s) => ({
                urls: s.urls,
                username: s.username ?? null,
                credential: s.credential ?? null,
            })),
            simLatency: this.simLatency,
            simPdv: this.simPdv,
            simPacketLoss: this.simPacketLoss,
            leaveReason: this.leaveReason,
            peerCount: this.peerCount,
            peersList: this.peersList,
        };
    }
}

/**
 * Construct 3 P2P 客戶端管理器 - 整合版本
 */
class Construct3P2PClient {
    constructor() {
        this._eventManager = new EventManager();
        this.clients = new Map();
        this.currentClient = null;
        this.currentRoom = null;
        this.peers = new Map();
        this.isReady = false;
        
        // 初始化Ed25519密鑰管理器
        this.keyManager = new Ed25519KeyManager();
        
        // 設置事件監聽
        this.setupEventListeners();
        
        // 自動加載存儲的密鑰
        this.keyManager.loadFromStorage();
    }

    /**
     * 設置事件監聽器（只設置一次）
     */
    setupEventListeners() {
        if (this._listenersSetup) {
            return; // 避免重複設置
        }
        
        const eventManager = this._eventManager;
        
        // 連接事件
        eventManager.on('connected', (data) => {
            console.log('已連接到信令服務器:', data);
            this.onConnected?.(data);
        });
        
        eventManager.on('loggedIn', (data) => {
            console.log('已登錄:', data);
            this.onLoggedIn?.(data);
        });
        
        eventManager.on('joinedRoom', (data) => {
            console.log('已加入房間:', data);
            this.currentRoom = data;
            this.isReady = true;
            this.onJoinedRoom?.(data);
        });
        
        eventManager.on('leftRoom', (data) => {
            console.log('已離開房間:', data);
            this.currentRoom = null;
            this.isReady = false;
            this.peers.clear();
            this.onLeftRoom?.(data);
        });
        
        // 對等節點事件
        eventManager.on('peerJoined', (data) => {
            console.log('對等節點加入:', data);
            this.peers.set(data.peerId, {
                id: data.peerId,
                alias: data.peerAlias,
                joinedAt: Date.now()
            });
            this.onPeerJoined?.(data);
        });
        
        eventManager.on('onPeerDisconnected', (data) => {
            console.log('對等節點離開:', data);
            this.peers.delete(data.peerId);
            this.onPeerDisconnected?.(data);
        });
        
        // 消息事件
        eventManager.on('peerMessage', (data) => {
            console.log('收到對等節點消息:', data);
            this.onPeerMessage?.(data);
        });
        
        // 錯誤事件
        eventManager.on('onError', (data) => {
            console.error('P2P 錯誤:', data);
            this.onError?.(data);
        });
        
        eventManager.on('onKicked', (data) => {
            console.log('被踢出房間:', data);
            this.onKicked?.(data);
        });
        
        // 房間列表事件
        eventManager.on('room-list', (data) => {
            console.log('房間列表:', data);
            this.onRoomList?.(data);
        });
        
        eventManager.on('instance-list', (data) => {
            console.log('實例列表:', data);
            this.onInstanceList?.(data);
        });
        
        this._listenersSetup = true; // 標記已設置
    }

    /**
     * 連接到信令服務器
     * @param {string} serverUrl - 服務器 URL
     * @param {string} tag - 客戶端標籤
     * @returns {P2PClient} 客戶端實例
     */
    connectToSignallingServer(serverUrl, tag) {
        let client = this.clients.get(tag);
        if (!client) {
            client = new P2PClient(tag, this._eventManager);
            this.clients.set(tag, client);
        }
        client.connectToSignallingServer(serverUrl);
        this.currentClient = client;
        return client;
    }

    /**
     * 連接到 Construct 3 多玩家服務器
     * @param {string} alias - 用戶別名
     * @param {string} clientTag - 客戶端標籤（可選）
     * @returns {Promise} 連接 Promise
     */
    async connect(alias, clientTag = 'main') {
        return new Promise((resolve, reject) => {
            try {
                // 連接到信令服務器
                this.currentClient = this.connectToSignallingServer('wss://multiplayer.construct.net', clientTag);
                
                // 等待連接建立
                const onConnected = (data) => {
                    if (data.clientTag === clientTag) {
                        this._eventManager.off('connected', onConnected);
                        this._eventManager.off('loggedIn', onLoggedIn);
                        
                        // 登錄
                        this.currentClient.loginToSignallingServer(alias);
                    }
                };
                
                const onLoggedIn = (data) => {
                    if (data.clientTag === clientTag) {
                        this._eventManager.off('connected', onConnected);
                        this._eventManager.off('loggedIn', onLoggedIn);
                        resolve(data);
                    }
                };
                
                this._eventManager.on('connected', onConnected);
                this._eventManager.on('loggedIn', onLoggedIn);
                
                // 設置超時
                setTimeout(() => {
                    this._eventManager.off('connected', onConnected);
                    this._eventManager.off('loggedIn', onLoggedIn);
                    reject(new Error('連接超時'));
                }, 10000);
                
            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * 加入房間 (信令服務器會決定先加入者為主機) (這裡可能存在很多錯誤有待修正)
     * @param {string} traceID - 追蹤 ID (可選，未提供時自動生成)
     * @param {string} publicKey - 公鑰 (可選，未提供時自動生成)
     * @param {string} idSign - ID 簽名 (可選，未提供時自動簽名peerId)
     * @param {number} maxPlayers - 最大玩家數量
     * @param {Object} options - 額外選項
     * @returns {Promise} 加入房間 Promise
     */
    async joinRoom(traceID, publicKey, idSign, maxPlayers = 8, options = {}) {
        const { keyId = null, autoGenerateKeys = true, clientTag = null } = options;
        
        // 確定要使用的客戶端
        const targetClient = clientTag ? this.getClient(clientTag) : this.currentClient;
        if (!targetClient) {
            throw new Error('未找到指定的客戶端');
        }
        
        // 如果指定了clientTag但客戶端不存在，創建新的客戶端
        if (clientTag && !targetClient) {
            this.connectToSignallingServer('wss://multiplayer.construct.net', clientTag);
        }
        
        let finalTraceID = traceID;
        let finalPublicKey = publicKey;
        let finalIdSign = idSign;
        
        // 如果沒有提供traceID，生成隨機的
        if (!finalTraceID) {
            finalTraceID = this.keyManager.generateTraceID();
        }
        
        // 如果沒有提供publicKey，需要生成密鑰對
        if (!finalPublicKey && autoGenerateKeys) {
            const keyPair = await this.keyManager.generateKeyPair(keyId);
            finalPublicKey = keyPair.publicKey;
            
            // 如果沒有提供idSign，使用peerId簽名生成
            if (!finalIdSign) {
                const peerId = targetClient.myid || 'temp_peer_id';
                finalIdSign = await this.keyManager.signData(keyPair.id, peerId);
            }
        }
        
        return new Promise((resolve, reject) => {
            let resolved = false;
            
            const onJoinedRoom = (data) => {
                if (data.clientTag === (clientTag || this.currentClient?.tag) && !resolved) {
                    resolved = true;
                    this._eventManager.off('joinedRoom', onJoinedRoom);
                    resolve(data);
                }
            };
            
            this._eventManager.on('joinedRoom', onJoinedRoom);
            
            try {
                targetClient.joinRoom(finalTraceID, finalPublicKey, finalIdSign, maxPlayers);
            } catch (error) {
                if (!resolved) {
                    resolved = true;
                    this._eventManager.off('joinedRoom', onJoinedRoom);
                    reject(error);
                }
            }
            
            // 設置超時
            setTimeout(() => {
                if (!resolved) {
                    resolved = true;
                    this._eventManager.off('joinedRoom', onJoinedRoom);
                    reject(new Error('加入房間超時'));
                }
            }, 10000);
        });
    }

    /**
     * 自動匹配房間 (信令服務器會決定先加入者為主機)
     * @param {string} traceID - 追蹤 ID (可選，未提供時自動生成)
     * @param {string} publicKey - 公鑰 (可選，未提供時自動生成)
     * @param {string} idSign - ID 簽名 (可選，未提供時自動簽名peerId)
     * @param {number} maxPlayers - 最大玩家數量
     * @param {boolean} lockWhenFull - 滿員時是否鎖定
     * @param {Object} options - 額外選項
     * @returns {Promise} 自動匹配 Promise
     */
    async autoMatch(traceID, publicKey, idSign, maxPlayers = 8, lockWhenFull = false, options = {}) {
        if (!this.currentClient) {
            throw new Error('未連接到服務器');
        }
        
        const { keyId = null, autoGenerateKeys = true } = options;
        
        let finalTraceID = traceID;
        let finalPublicKey = publicKey;
        let finalIdSign = idSign;
        
        // 如果沒有提供traceID，生成隨機的
        if (!finalTraceID) {
            finalTraceID = this.keyManager.generateTraceID();
        }
        
        // 如果沒有提供publicKey，需要生成密鑰對
        if (!finalPublicKey && autoGenerateKeys) {
            const keyPair = await this.keyManager.generateKeyPair(keyId);
            finalPublicKey = keyPair.publicKey;
            
            // 如果沒有提供idSign，使用peerId簽名生成
            if (!finalIdSign) {
                const peerId = this.currentClient.myid || 'temp_peer_id';
                finalIdSign = await this.keyManager.signData(keyPair.id, peerId);
            }
        }
        
        return new Promise((resolve, reject) => {
            const onJoinedRoom = (data) => {
                if (data.clientTag === this.currentClient.tag) {
                    this._eventManager.off('joinedRoom', onJoinedRoom);
                    resolve(data);
                }
            };
            
            this._eventManager.on('joinedRoom', onJoinedRoom);
            
            try {
                this.currentClient.autoJoinRoom(finalTraceID, finalPublicKey, finalIdSign, maxPlayers, lockWhenFull);
            } catch (error) {
                this._eventManager.off('joinedRoom', onJoinedRoom);
                reject(error);
            }
            
            // 設置超時
            setTimeout(() => {
                this._eventManager.off('joinedRoom', onJoinedRoom);
                reject(new Error('自動匹配超時'));
            }, 15000);
        });
    }

    /**
     * 發送聊天消息
     * @param {string} message - 消息內容
     * @param {string} channel - 通道類型 ('orderedReliable', 'unorderedReliable', 'unreliable')
     * @param {string} clientTag - 客戶端標籤（可選，不提供時使用當前客戶端）
     * @returns {boolean} 是否發送成功
     */
    sendChatMessage(message, channel = 'orderedReliable', clientTag = null) {
        const client = clientTag ? this.getClient(clientTag) : this.currentClient;
        
        if (!client) {
            console.warn('未找到指定的客戶端');
            return false;
        }
        
        if (!client.isOnRoom) {
            console.warn('客戶端未在房間中');
            return false;
        }
        
        const messageData = JSON.stringify({
            type: 'chat',
            message: message,
            timestamp: Date.now(),
            sender: client.myAlias
        });
        
        console.log('發送聊天消息:', messageData);
        
        if (client.isHost) {
            // 主機廣播消息
            client.broadcastMessageToPeers(client.myid, messageData, channel);
        } else {
            // 客戶端發送給主機
            client.sendMessageToPeer('', messageData, channel);
        }
        
        return true;
    }

    /**
     * 發送遊戲數據
     * @param {Object} data - 遊戲數據
     * @param {string} channel - 通道類型
     * @returns {boolean} 是否發送成功
     */
    sendGameData(data, channel = 'orderedReliable') {
        if (!this.currentClient || !this.isReady) {
            console.warn('未準備好發送遊戲數據');
            return false;
        }
        
        const messageData = JSON.stringify({
            type: 'gameData',
            data: data,
            timestamp: Date.now(),
            sender: this.currentClient.myid
        });
        
        if (this.currentClient.isHost) {
            this.currentClient.broadcastMessageToPeers(this.currentClient.myid, messageData, channel);
        } else {
            this.currentClient.sendMessageToPeer('', messageData, channel);
        }
        
        return true;
    }

    /**
     * 發送私人消息
     * @param {string} peerId - 對等節點 ID
     * @param {string} message - 消息內容
     * @param {string} channel - 通道類型
     * @returns {boolean} 是否發送成功
     */
    sendPrivateMessage(peerId, message, channel = 'orderedReliable') {
        if (!this.currentClient || !this.isReady) {
            console.warn('未準備好發送私人消息');
            return false;
        }
        
        const messageData = JSON.stringify({
            type: 'privateMessage',
            message: message,
            timestamp: Date.now(),
            sender: this.currentClient.myAlias
        });
        
        this.currentClient.sendMessageToPeer(peerId, messageData, channel);
        return true;
    }

    /**
     * 踢出玩家
     * @param {string} peerId - 玩家 ID
     * @param {string} reason - 踢出原因
     * @returns {boolean} 是否踢出成功
     */
    kickPlayer(peerId, reason = '被管理員踢出') {
        if (!this.currentClient || !this.isReady || !this.currentClient.isHost) {
            console.warn('只有主機可以踢出玩家');
            return false;
        }
        
        this.currentClient.kickPeer(peerId, reason);
        return true;
    }

    /**
     * 離開房間
     */
    leaveRoom() {
        if (this.currentClient) {
            this.currentClient.disconnectFromRoom();
        }
    }

    /**
     * 斷開連接
     */
    disconnect() {
        if (this.currentClient) {
            this.currentClient.disconnectFromSignalling();
        }
        this.currentClient = null;
        this.currentRoom = null;
        this.peers.clear();
        this.isReady = false;
    }

    /**
     * 獲取房間信息
     * @returns {Object|null} 房間信息
     */
    getRoomInfo() {
        if (!this.currentClient) return null;
        
        return {
            traceID: this.currentClient.traceID,
            publicKey: this.currentClient.publicKey,
            idSign: this.currentClient.idSign,
            isHost: this.currentClient.isHost,
            hostId: this.currentClient.hostId,
            hostAlias: this.currentClient.hostAlias,
            peerCount: this.peers.size + 1, // +1 for self
            peers: Array.from(this.peers.values())
        };
    }

    /**
     * 獲取當前用戶信息
     * @returns {Object|null} 用戶信息
     */
    getUserInfo() {
        if (!this.currentClient) return null;
        
        return {
            id: this.currentClient.myid,
            alias: this.currentClient.myAlias,
            isConnected: this.currentClient.isConnected,
            isLoggedIn: this.currentClient.isLoggedIn,
            isHost: this.currentClient.isHost,
            isInRoom: this.currentClient.isOnRoom
        };
    }

    /**
     * 請求房間列表
     * @param {string} traceID - 追蹤 ID
     * @param {string} publicKey - 公鑰
     * @param {string} which - 查詢類型
     */
    requestRoomList(traceID, publicKey, which = 'all') {
        if (this.currentClient) {
            this.currentClient.requestRoomList(traceID, publicKey, which);
        }
    }

    /**
     * 請求實例列表
     * @param {string} traceID - 追蹤 ID
     */
    requestInstanceList(traceID) {
        if (this.currentClient) {
            this.currentClient.requestInstanceList(traceID);
        }
    }

    /**
     * 設置網絡模擬參數
     * @param {number} latency - 延遲（毫秒）
     * @param {number} pdv - 延遲變化（毫秒）
     * @param {number} loss - 丟包率（百分比）
     */
    setNetworkSimulation(latency = 0, pdv = 0, loss = 0) {
        if (this.currentClient) {
            this.currentClient.setSimulationParams(latency, pdv, loss);
        }
    }

    /**
     * 獲取客戶端
     * @param {string} tag - 客戶端標籤
     * @returns {P2PClient|null} 客戶端實例
     */
    getClient(tag) {
        return this.clients.get(tag);
    }

    /**
     * 移除客戶端
     * @param {string} tag - 客戶端標籤
     */
    removeClient(tag) {
        const client = this.clients.get(tag);
        if (client) {
            client.disconnectFromSignalling();
            this.clients.delete(tag);
        }
    }

    /**
     * 獲取所有客戶端
     * @returns {Map} 客戶端映射
     */
    getAllClients() {
        return this.clients;
    }

    /**
     * 密鑰管理方法
     */
    
    /**
     * 生成新的Ed25519密鑰對
     * @param {string} keyId - 密鑰ID（可選）
     * @returns {Promise<Object>} 密鑰對對象
     */
    async generateKeyPair(keyId = null) {
        return await this.keyManager.generateKeyPair(keyId);
    }

    /**
     * 使用私鑰簽名數據
     * @param {string} keyId - 密鑰ID
     * @param {string} data - 要簽名的數據
     * @returns {Promise<string>} 簽名結果（Base64）
     */
    async signData(keyId, data) {
        return await this.keyManager.signData(keyId, data);
    }

    /**
     * 驗證簽名
     * @param {string} keyId - 密鑰ID
     * @param {string} data - 原始數據
     * @param {string} signature - 簽名（Base64）
     * @returns {Promise<boolean>} 驗證結果
     */
    async verifySignature(keyId, data, signature) {
        return await this.keyManager.verifySignature(keyId, data, signature);
    }

    /**
     * 獲取密鑰對
     * @param {string} keyId - 密鑰ID
     * @returns {Object|null} 密鑰對數據
     */
    getKeyPair(keyId) {
        return this.keyManager.getKeyPair(keyId);
    }

    /**
     * 獲取所有密鑰對
     * @returns {Array} 所有密鑰對列表
     */
    getAllKeyPairs() {
        return this.keyManager.getAllKeyPairs();
    }

    /**
     * 刪除密鑰對
     * @param {string} keyId - 密鑰ID
     * @returns {boolean} 是否刪除成功
     */
    async deleteKeyPair(keyId) {
        return await this.keyManager.deleteKeyPair(keyId);
    }

    /**
     * 生成隨機traceID
     * @returns {string} 隨機traceID
     */
    generateTraceID() {
        return this.keyManager.generateTraceID();
    }

    /**
     * 事件管理
     */
    get eventManager() {
        return this._eventManager;
    }

    // 事件回調函數（由用戶設置）
    onConnected = null;
    onLoggedIn = null;
    onJoinedRoom = null;
    onLeftRoom = null;
    onPeerJoined = null;
    onPeerDisconnected = null;
    onPeerMessage = null;
    onError = null;
    onKicked = null;
    onRoomList = null;
    onInstanceList = null;
}

/**
 * Ed25519 密鑰管理類
 */
class Ed25519KeyManager {
    constructor() {
        this.keyPairs = new Map(); // 存儲密鑰對
        this.storageKey = 'construct3_p2p_keys';
    }

    /**
     * 生成新的Ed25519密鑰對
     * @param {string} keyId - 密鑰ID（可選）
     * @returns {Promise<Object>} 密鑰對對象
     */
    async generateKeyPair(keyId = null) {
        try {
            // 使用Web Crypto API生成Ed25519密鑰對
            const keyPair = await crypto.subtle.generateKey(
                {
                    name: "Ed25519",
                    namedCurve: "Ed25519"
                },
                true, // 可導出
                ["sign", "verify"]
            );

            // 導出公鑰和私鑰
            const publicKey = await crypto.subtle.exportKey("raw", keyPair.publicKey);
            const privateKey = await crypto.subtle.exportKey("pkcs8", keyPair.privateKey);

            // 轉換為Base64字符串
            const publicKeyBase64 = this.arrayBufferToBase64(publicKey);
            const privateKeyBase64 = this.arrayBufferToBase64(privateKey);

            const keyData = {
                id: keyId || this.generateKeyId(),
                publicKey: publicKeyBase64,
                privateKey: privateKeyBase64,
                createdAt: Date.now(),
                keyPair: keyPair // 保留原始密鑰對用於簽名
            };

            // 存儲密鑰對
            this.keyPairs.set(keyData.id, keyData);
            await this.saveToStorage();

            return {
                id: keyData.id,
                publicKey: publicKeyBase64,
                privateKey: privateKeyBase64,
                createdAt: keyData.createdAt
            };
        } catch (error) {
            console.error('生成Ed25519密鑰對失敗:', error);
            throw new Error('無法生成Ed25519密鑰對');
        }
    }

    /**
     * 使用私鑰簽名數據
     * @param {string} keyId - 密鑰ID
     * @param {string} data - 要簽名的數據
     * @returns {Promise<string>} 簽名結果（Base64）
     */
    async signData(keyId, data) {
        try {
            const keyData = this.keyPairs.get(keyId);
            if (!keyData) {
                throw new Error(`找不到密鑰對: ${keyId}`);
            }

            // 將數據轉換為ArrayBuffer
            const dataBuffer = new TextEncoder().encode(data);

            // 使用私鑰簽名
            const signature = await crypto.subtle.sign(
                "Ed25519",
                keyData.keyPair.privateKey,
                dataBuffer
            );

            return this.arrayBufferToBase64(signature);
        } catch (error) {
            console.error('簽名失敗:', error);
            throw new Error('簽名失敗');
        }
    }

    /**
     * 驗證簽名
     * @param {string} keyId - 密鑰ID
     * @param {string} data - 原始數據
     * @param {string} signature - 簽名（Base64）
     * @returns {Promise<boolean>} 驗證結果
     */
    async verifySignature(keyId, data, signature) {
        try {
            const keyData = this.keyPairs.get(keyId);
            if (!keyData) {
                return false;
            }

            const dataBuffer = new TextEncoder().encode(data);
            const signatureBuffer = this.base64ToArrayBuffer(signature);

            return await crypto.subtle.verify(
                "Ed25519",
                keyData.keyPair.publicKey,
                signatureBuffer,
                dataBuffer
            );
        } catch (error) {
            console.error('驗證簽名失敗:', error);
            return false;
        }
    }

    /**
     * 獲取密鑰對
     * @param {string} keyId - 密鑰ID
     * @returns {Object|null} 密鑰對數據
     */
    getKeyPair(keyId) {
        const keyData = this.keyPairs.get(keyId);
        if (!keyData) return null;

        return {
            id: keyData.id,
            publicKey: keyData.publicKey,
            privateKey: keyData.privateKey,
            createdAt: keyData.createdAt
        };
    }

    /**
     * 獲取所有密鑰對
     * @returns {Array} 所有密鑰對列表
     */
    getAllKeyPairs() {
        return Array.from(this.keyPairs.values()).map(keyData => ({
            id: keyData.id,
            publicKey: keyData.publicKey,
            createdAt: keyData.createdAt
        }));
    }

    /**
     * 刪除密鑰對
     * @param {string} keyId - 密鑰ID
     * @returns {boolean} 是否刪除成功
     */
    async deleteKeyPair(keyId) {
        if (this.keyPairs.delete(keyId)) {
            await this.saveToStorage();
            return true;
        }
        return false;
    }

    /**
     * 從存儲中加載密鑰對
     */
    async loadFromStorage() {
        try {
            if (typeof localStorage !== 'undefined') {
                const stored = localStorage.getItem(this.storageKey);
                if (stored) {
                    const keyDataArray = JSON.parse(stored);
                    for (const keyData of keyDataArray) {
                        // 重新導入密鑰對
                        const publicKey = await crypto.subtle.importKey(
                            "raw",
                            this.base64ToArrayBuffer(keyData.publicKey),
                            { name: "Ed25519" },
                            true,
                            ["verify"]
                        );

                        const privateKey = await crypto.subtle.importKey(
                            "pkcs8",
                            this.base64ToArrayBuffer(keyData.privateKey),
                            { name: "Ed25519" },
                            true,
                            ["sign"]
                        );

                        const keyPair = {
                            publicKey: publicKey,
                            privateKey: privateKey
                        };

                        this.keyPairs.set(keyData.id, {
                            ...keyData,
                            keyPair: keyPair
                        });
                    }
                }
            }
        } catch (error) {
            console.error('從存儲加載密鑰對失敗:', error);
        }
    }

    /**
     * 保存
     */
    async saveToStorage() {
        try {
            if (typeof localStorage !== 'undefined') {
                const keyDataArray = Array.from(this.keyPairs.values()).map(keyData => ({
                    id: keyData.id,
                    publicKey: keyData.publicKey,
                    privateKey: keyData.privateKey,
                    createdAt: keyData.createdAt
                }));
                localStorage.setItem(this.storageKey, JSON.stringify(keyDataArray));
            }
        } catch (error) {
            console.error('保存密鑰失敗:', error);
        }
    }

    /**
     * 生成密鑰ID
     * @returns {string} 隨機密鑰ID
     */
    generateKeyId() {
        return 'key_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
    }

    /**
     * 生成隨機traceID
     * @returns {string} 隨機traceID
     */
    generateTraceID() {
        return 'trace_' + Date.now() + '_' + Math.random().toString(36).substr(2, 12);
    }

    /**
     * ArrayBuffer轉Base64
     * @param {ArrayBuffer} buffer - ArrayBuffer
     * @returns {string} Base64字符串
     */
    arrayBufferToBase64(buffer) {
        const bytes = new Uint8Array(buffer);
        let binary = '';
        for (let i = 0; i < bytes.byteLength; i++) {
            binary += String.fromCharCode(bytes[i]);
        }
        return btoa(binary);
    }

    /**
     * Base64轉ArrayBuffer
     * @param {string} base64 - Base64字符串
     * @returns {ArrayBuffer} ArrayBuffer
     */
    base64ToArrayBuffer(base64) {
        const binary = atob(base64);
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i++) {
            bytes[i] = binary.charCodeAt(i);
        }
        return bytes.buffer;
    }
}

// 導出類別
if (typeof window !== 'undefined') {
    // 瀏覽器環境
    window.Construct3P2PClient = Construct3P2PClient;
    window.P2PClient = P2PClient;
    window.EventManager = EventManager;
    window.ChannelSendQueue = ChannelSendQueue;
    window.Ed25519KeyManager = Ed25519KeyManager;
}
