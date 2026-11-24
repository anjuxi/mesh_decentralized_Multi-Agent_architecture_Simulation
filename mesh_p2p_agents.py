import random;
import uuid;
from collections import deque;
from dataclasses import dataclass, field;

# 调参都在这，改完直接跑。
V_消息最大跳数 = 5;      # TTL，消息最多转发5跳就丢，防泛洪打爆全网。
V_共识通过票数 = 3;      # 5个节点取多数，拿到3票就算谈拢。
V_单议题最大轮数 = 6;    # 单个议题最多谈6轮，兜底防跑飞。
V_初始节点数 = 5;
V_每节点额外连边数 = 1;  # 环形骨架外随机补的边，凑成部分网状，故意不全连。
V_随机种子 = 42;         # 固定种子，每次跑结果一样，方便对照。
V_策略候选集 = ["进攻", "防守", "游击"];
V_详细日志 = False;      # 打开能看到每一跳投递，量大，慎开。
V_日志开关 = True;

@dataclass
class MeshMsg:
    # 全网流转的消息信封，HELLO/PROPOSE/VOTE/RESULT四类共用一个壳。
    msg_type: str;    # HELLO邻居发现 / PROPOSE提案 / VOTE投票 / RESULT共识结果。
    sender_id: str;   # 消息源头，出问题好追溯。
    content: str;     # 正文：提案和投票放策略名，RESULT放结论。
    round_no: int;    # 所属协商轮次，防跨轮串数据。
    msg_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8]);  # 去重全靠这个ID。
    ttl: int = V_消息最大跳数;  # 每转发一跳减1，减到0就丢。

def log(text):
    # 统一日志出口，收发消息、投票记录都从这走，方便回溯。参数text是日志文本，返回值无。
    if V_日志开关:
        print(text);

class PeerAgent:
    # 对等节点：自己收消息、发消息、拿主意，谁也不听指挥。

    def __init__(self, peer_id, preference):
        # 起一个对等节点，peer_id是唯一标识，preference是初始策略偏好，返回值无。
        self.peer_id = peer_id;
        self.preference = preference;  # 当前偏好，谈判时往多数派挪。
        self.neighbors = {};           # 邻居表：对端ID -> 投递函数，点对点链路全登记在这。
        self.inbox = deque();          # 收件箱，投来的消息先排队。
        self.seen_ids = set();         # 处理过的消息ID，去重就查这里。
        self.dedup_dropped = 0;        # 拦下的重复消息数。
        self.peer_proposals = {};      # 本轮收到的提案：发送方 -> 策略。
        self.vote_tally = {};          # 本轮计票：策略 -> 票数，各记各的，没有中心计票员。
        self.final_result = None;      # 谈拢的结论，先到先得。

    def bind_neighbor(self, peer_id, deliver_fn):
        # 往邻居表挂一条点对点链路。参数peer_id是对端ID，deliver_fn是投递函数，返回值无。
        self.neighbors[peer_id] = deliver_fn;

    def unicast(self, peer_id, msg):
        # 单发给指定邻居，不走泛洪，给新节点同步结果时用。参数peer_id是目标邻居，msg是消息，返回值无。
        deliver_fn = self.neighbors.get(peer_id);
        if deliver_fn is not None:
            deliver_fn(msg, self.peer_id);

    def flood(self, msg, exclude_id=None):
        # 泛洪：发给除来源方向外的全部邻居，网状扩散就靠这个，exclude_id防消息原路兜圈。
        # 参数msg是消息，exclude_id是要排除的对端ID，返回值无。
        for peer_id, deliver_fn in self.neighbors.items():
            if peer_id != exclude_id:
                deliver_fn(msg, self.peer_id);

    def process_inbox(self):
        # 清收件箱：去重、查跳数、转发，然后按消息类型处理业务。参数和返回值均无。
        while self.inbox:
            msg, from_id = self.inbox.popleft();
            if msg.msg_id in self.seen_ids:
                # 同一条消息会从好几条路径挤进来，去重是防风暴的第一道闸。
                self.dedup_dropped += 1;
                continue;
            self.seen_ids.add(msg.msg_id);
            if msg.ttl <= 0:
                # 跳数耗尽直接丢，不给它继续转发的机会。
                log(f"    [{self.peer_id}] 消息{msg.msg_id}跳数耗尽，停止转发！");
                continue;
            if V_详细日志:
                log(f"    [{self.peer_id}] 收到{msg.msg_type}（来自{from_id}，id={msg.msg_id}，剩{msg.ttl}跳）");
            # 转发给除来源外的邻居，跳数减1。
            forward = MeshMsg(msg_type=msg.msg_type, sender_id=msg.sender_id, content=msg.content, round_no=msg.round_no, msg_id=msg.msg_id, ttl=msg.ttl - 1);
            self.flood(forward, exclude_id=from_id);
            # 自己的业务处理，按类型分流。
            if msg.msg_type == "HELLO":
                self.on_hello(msg);
            elif msg.msg_type == "PROPOSE":
                self.peer_proposals[msg.sender_id] = msg.content;
            elif msg.msg_type == "VOTE":
                self.vote_tally[msg.content] = self.vote_tally.get(msg.content, 0) + 1;
            elif msg.msg_type == "RESULT":
                if self.final_result is None:
                    # 结论只认先到的，全网自然对齐到同一个说法。
                    self.final_result = msg.content;
                    log(f"    [{self.peer_id}] 已同步共识结果：{msg.content}！");

    def on_hello(self, msg):
        # 有人入网打招呼，手里有结论就定向补发一份RESULT给他。参数msg是HELLO消息，返回值无。
        if self.final_result is not None:
            sync = MeshMsg(msg_type="RESULT", sender_id=self.peer_id, content=self.final_result, round_no=msg.round_no);
            self.unicast(msg.sender_id, sync);
            log(f"    [{self.peer_id}] 给新节点{msg.sender_id}补发了共识结果：{self.final_result}！");

    def begin_issue(self, preference):
        # 开新议题，重置议题相关的状态。参数preference是本议题的初始偏好，返回值无。
        self.preference = preference;
        self.peer_proposals = {};
        self.vote_tally = {};
        self.final_result = None;

    def begin_round(self):
        # 每轮开头清掉上一轮的提案和票，防串数据。参数和返回值均无。
        self.peer_proposals = {};
        self.vote_tally = {};

    def broadcast_propose(self, round_no):
        # 把当前偏好包装成提案泛洪出去。参数round_no是轮次号，返回值无。
        msg = MeshMsg(msg_type="PROPOSE", sender_id=self.peer_id, content=self.preference, round_no=round_no);
        log(f"    [{self.peer_id}] 提案：{self.preference}！");
        self.flood(msg);

    def negotiate(self):
        # 谈判步：数一数邻居的提案，往多数派挪。平票随机选一边接着谈，
        # 偶数节点对半开的僵局就靠轮次间的随机性破掉，全局一致就是这么慢慢磨出来的。
        # 参数和返回值均无。
        tally = {self.preference: 1};
        for strategy in self.peer_proposals.values():
            tally[strategy] = tally.get(strategy, 0) + 1;
        top_votes = max(tally.values());
        winners = [s for s, v in tally.items() if v == top_votes];
        best = random.choice(winners);
        if best != self.preference:
            self.preference = best;
        log(f"    [{self.peer_id}] 谈判后偏好：{self.preference}（收到{len(self.peer_proposals)}条邻居提案）！");

    def broadcast_vote(self, round_no):
        # 把谈判后的偏好当选票泛洪出去，自己那票也记进票箱。参数round_no是轮次号，返回值无。
        msg = MeshMsg(msg_type="VOTE", sender_id=self.peer_id, content=self.preference, round_no=round_no);
        log(f"    [{self.peer_id}] 投票：{self.preference}！");
        self.flood(msg);
        self.vote_tally[self.preference] = self.vote_tally.get(self.preference, 0) + 1;

    def check_consensus(self, round_no):
        # 看一眼自己的票箱，哪个策略凑够票数就宣布结果、泛洪RESULT。
        # 参数round_no是轮次号，返回值无，结论落在self.final_result上。
        if self.final_result is not None:
            return;
        for strategy, votes in self.vote_tally.items():
            if votes >= V_共识通过票数:
                self.final_result = strategy;
                log(f"    [{self.peer_id}] 「{strategy}」拿到{votes}票，够数了，宣布结果！");
                self.flood(MeshMsg(msg_type="RESULT", sender_id=self.peer_id, content=strategy, round_no=round_no));
                return;

class MeshNetwork:
    # 网络层只管两件事：记着谁在线、把消息送到。不调度，也没东西可调度。

    def __init__(self):
        # 参数和返回值均无。
        self.agents = {};      # 在线节点登记：节点ID -> PeerAgent。
        self.delivered = 0;    # 成功投递总数，看通信开销用。
        self.drop_failed = 0;  # 目标下线导致投递失败的条数。

    def add_agent(self, agent):
        # 登记节点进网。参数agent是PeerAgent对象，返回值无。
        self.agents[agent.peer_id] = agent;

    def remove_agent(self, peer_id):
        # 节点下线（模拟宕机）：只摘登记，发往它的消息会在deliver里自然失败。
        # 参数peer_id是要下线的节点ID，返回值无。
        if peer_id in self.agents:
            del self.agents[peer_id];

    def connect(self, id_a, id_b):
        # 建双向点对点链路，两端互写邻居表。参数id_a、id_b是两端节点ID，返回值无。
        # lambda里用默认参数把tgt锁死，不然闭包共享变量会串线，这是踩过的坑。
        self.agents[id_a].bind_neighbor(id_b, lambda msg, frm, tgt=id_b: self.deliver(tgt, msg, frm));
        self.agents[id_b].bind_neighbor(id_a, lambda msg, frm, tgt=id_a: self.deliver(tgt, msg, frm));

    def deliver(self, to_id, msg, from_id):
        # 模拟网线：把消息投进目标收件箱。目标下线就丢弃——断一条链路而已，全网照跑。
        # 参数to_id是目标节点，msg是消息，from_id是上一跳来源，返回值无。
        target = self.agents.get(to_id);
        if target is None:
            self.drop_failed += 1;
            if V_详细日志:
                log(f"    [链路] {from_id} -> {to_id} 投递失败：目标已下线，消息{msg.msg_id}丢弃！");
            return;
        self.delivered += 1;
        target.inbox.append((msg, from_id));

    def flush_all(self):
        # 轮转清空全网收件箱。泛洪会连锁产生新投递，得循环到彻底消停为止。
        # 参数和返回值均无。
        while any(agent.inbox for agent in self.agents.values()):
            for agent in list(self.agents.values()):
                agent.process_inbox();

    def online_ids(self):
        # 列出在线节点ID。参数无，返回节点ID列表。
        return list(self.agents.keys());

# ---- 下面是运行编排 ----

def build_mesh(node_count):
    # 建部分网状拓扑：先连个环保证连通，再随机补边加密度。
    # 参数node_count是入网节点数，返回网络对象和各节点初始偏好字典。
    network = MeshNetwork();
    preferences = {};
    for i in range(node_count):
        peer_id = chr(ord("A") + i);
        agent = PeerAgent(peer_id, random.choice(V_策略候选集));
        preferences[peer_id] = agent.preference;
        network.add_agent(agent);
    ids = network.online_ids();
    for i in range(len(ids)):
        # 环形骨架A-B-C-D-E-A，保底连通，泛洪能覆盖全网。
        network.connect(ids[i], ids[(i + 1) % len(ids)]);
    for peer_id in ids:
        for _ in range(V_每节点额外连边数):
            other = random.choice(ids);
            if other != peer_id and other not in network.agents[peer_id].neighbors:
                network.connect(peer_id, other);
    return network, preferences;

def run_negotiation_round(network, round_no):
    # 跑一轮完整协商：清状态、提案、谈判、投票、计票、宣布结论。
    # 参数network是网络对象，round_no是轮次号，返回值无。
    log(f"—— 协商第{round_no}轮 ——");
    for agent in network.agents.values():
        agent.begin_round();
    for agent in network.agents.values():
        agent.broadcast_propose(round_no);
    network.flush_all();
    for agent in network.agents.values():
        agent.negotiate();
    for agent in network.agents.values():
        agent.broadcast_vote(round_no);
    network.flush_all();
    for agent in network.agents.values():
        agent.check_consensus(round_no);
    network.flush_all();

def negotiate_until_consensus(network, start_round):
    # 一轮接一轮谈，直到所有在线节点都拿到同一个结论。
    # 参数network是网络对象，start_round是起始轮次号，返回本议题实际用掉的轮数。
    round_no = start_round;
    while round_no - start_round < V_单议题最大轮数:
        run_negotiation_round(network, round_no);
        if all(agent.final_result for agent in network.agents.values()):
            break;
        round_no += 1;
    return round_no - start_round + 1;

def join_network(network, peer_id, preference, neighbor_ids):
    # 新节点入网：登记、连边、广播HELLO，等邻居定向回传RESULT完成同步。
    # 参数network是网络对象，peer_id是新节点ID，preference是初始偏好，neighbor_ids是要连的邻居列表。
    # 返回新节点对象。
    agent = PeerAgent(peer_id, preference);
    network.add_agent(agent);
    for nid in neighbor_ids:
        network.connect(peer_id, nid);
    agent.flood(MeshMsg(msg_type="HELLO", sender_id=peer_id, content="入网报到", round_no=0));
    network.flush_all();
    return agent;

def main():
    # 主流程：建网、邻居发现、议题一协商、宕机演练、新节点入网、议题二协商、收尾统计。
    # 参数和返回值均无。
    random.seed(V_随机种子);
    print("网状去中心化多智能体架构（Mesh P2P）仿真开始！");

    network, preferences = build_mesh(V_初始节点数);
    print("——"*50);
    print("—— 第一步：建网，部分网状，故意不全连 ——");
    for peer_id, pref in preferences.items():
        neighbor_ids = list(network.agents[peer_id].neighbors.keys());
        print(f"节点{peer_id} 初始偏好：{pref}，邻居：{neighbor_ids}！");
    print(f"共{len(network.agents)}个对等节点入网，没有中心！");

    print("——"*50);
    print("—— 第二步：HELLO泛洪，认认邻居 ——");
    for agent in network.agents.values():
        agent.flood(MeshMsg(msg_type="HELLO", sender_id=agent.peer_id, content="上线报到", round_no=0));
    network.flush_all();
    print("HELLO泛洪完毕，点对点链路确认！");

    print("——"*50);
    print("—— 第三步：议题一「集群统一策略」，提案-谈判-投票 ——");
    used_rounds = negotiate_until_consensus(network, 1);
    results = set(agent.final_result for agent in network.agents.values());
    print(f"议题一谈拢：{results.pop()}，用了{used_rounds}轮！");

    print("——"*50);
    print("—— 第四步：鲁棒性演练，节点C宕机下线 ——");
    network.remove_agent("C");
    print(f"节点C下线，在线还剩：{','.join(network.online_ids())}！发往C的链路会投递失败，但全网照跑！");

    print("——"*50);
    print("—— 第五步：新节点F入网 ——");
    join_network(network, "F", random.choice(V_策略候选集), ["A", "D"]);
    print("节点F连上A、D并广播HELLO，老节点把结论定向同步给了它！");

    print("——"*50);
    print("—— 第六步：议题二「下一阶段转移阵地」，看上下线之后还能不能谈拢 ——");
    for peer_id in network.online_ids():
        network.agents[peer_id].begin_issue(random.choice(V_策略候选集));
    used_rounds = negotiate_until_consensus(network, 1);
    results = set(agent.final_result for agent in network.agents.values());
    print(f"议题二谈拢：{results.pop()}，用了{used_rounds}轮！");

    print("——"*50);
    total_dedup = sum(agent.dedup_dropped for agent in network.agents.values());
    print(f"全网共投递{network.delivered}条消息——节点两两直连就是这个代价，人越多涨得越快！");
    print(f"去重拦下{total_dedup}条重复消息，{network.drop_failed}条因目标下线投递失败！");
    print("✅ 没中心、没共享存储，结论是节点自己谈出来的，仿真收工！");

if __name__ == "__main__":
    main();
