import random;
import sys;
import mesh_p2p_agents as mesh;

# 跑测试时关掉主程序的日志，不然刷屏。
mesh.V_日志开关 = False;

def make_msg(content="测试内容", msg_type="PROPOSE", ttl=None):
    # 造一条测试消息。参数content是正文，msg_type是类型，ttl是跳数（None走默认），返回MeshMsg。
    if ttl is None:
        return mesh.MeshMsg(msg_type=msg_type, sender_id="T", content=content, round_no=1);
    return mesh.MeshMsg(msg_type=msg_type, sender_id="T", content=content, round_no=1, ttl=ttl);

def build_chain():
    # 搭A-B-C三节点链式拓扑，传播路径只有一条，投递次数可以精确断言。
    # 参数无，返回MeshNetwork。
    network = mesh.MeshNetwork();
    for peer_id in ["A", "B", "C"]:
        network.add_agent(mesh.PeerAgent(peer_id, "进攻"));
    network.connect("A", "B");
    network.connect("B", "C");
    return network;

def test_dedup():
    # 消息去重：同一消息从两条路挤进收件箱，只处理一次，转发也只发一次。参数和返回值均无。
    network = build_chain();
    b, c = network.agents["B"], network.agents["C"];
    msg = make_msg("去重验证");
    # 同一条消息塞两次，模拟从两条路径到达。
    b.inbox.append((msg, "A"));
    b.inbox.append((msg, "C"));
    network.flush_all();
    assert b.seen_ids == {msg.msg_id}, "B应只处理一次";
    assert b.dedup_dropped >= 1, "重复消息应被拦下";
    assert c.seen_ids == {msg.msg_id}, "C应收到一次转发";
    assert len(c.inbox) == 0, "C的收件箱应已清空";

def test_ttl():
    # TTL衰减：ttl=1的消息经B转一跳就归零，到C被丢弃，不再外传。参数和返回值均无。
    network = build_chain();
    b, c = network.agents["B"], network.agents["C"];
    msg = make_msg("跳数验证", ttl=1);
    b.inbox.append((msg, "A"));
    network.flush_all();
    assert msg.msg_id in c.seen_ids, "C应见过这条消息";
    assert c.final_result is None, "C不该产生任何业务副作用";
    assert network.delivered == 1, f"应恰好1次投递，实际{network.delivered}次";

def test_consensus():
    # 协商共识：5节点部分网状拓扑，谈几轮之后全网拿到同一个结论。参数和返回值均无。
    random.seed(mesh.V_随机种子);
    network, _ = mesh.build_mesh(mesh.V_初始节点数);
    mesh.negotiate_until_consensus(network, 1);
    results = set(agent.final_result for agent in network.agents.values());
    assert len(results) == 1, f"全网应只有一个结论，实际{results}";
    assert results.pop() in mesh.V_策略候选集, "结论应是候选集里的策略";

def test_node_down():
    # 节点下线：C宕机后剩下的人照常谈拢，不抛异常。参数和返回值均无。
    random.seed(mesh.V_随机种子 + 1);
    network, _ = mesh.build_mesh(mesh.V_初始节点数);
    network.remove_agent("C");
    assert "C" not in network.agents, "C应已下线";
    # 发往C的链路照常发包，投递在deliver里自然失败，不应炸。
    mesh.negotiate_until_consensus(network, 1);
    remaining = set(agent.final_result for agent in network.agents.values());
    assert len(remaining) == 1 and None not in remaining, f"剩余节点应谈拢，实际{remaining}";

def test_new_node_sync():
    # 新节点入网：F广播HELLO后，手里有结论的邻居定向回传RESULT。参数和返回值均无。
    random.seed(mesh.V_随机种子 + 2);
    network, _ = mesh.build_mesh(mesh.V_初始节点数);
    mesh.negotiate_until_consensus(network, 1);
    old_result = network.agents["A"].final_result;
    assert old_result is not None, "入网前全网应已谈拢";
    new_agent = mesh.join_network(network, "F", "游击", ["A", "B"]);
    assert set(new_agent.neighbors.keys()) == {"A", "B"}, "F的邻居应恰好是A和B";
    assert new_agent.final_result == old_result, "F应通过HELLO同步拿到全网结论";

def main():
    # 依次跑全部用例，汇总结论。参数和返回值均无。
    cases = [
        ("消息去重：多路径重复消息只处理一次", test_dedup),
        ("TTL衰减：跳数耗尽即丢弃", test_ttl),
        ("协商共识：提案谈判投票全网收敛", test_consensus),
        ("节点下线：宕机不瘫痪系统", test_node_down),
        ("新节点入网：HELLO触发定向同步", test_new_node_sync),
    ];
    print("网状去中心化架构仿真——功能测试开始！");
    failed = [];
    for name, case in cases:
        print("——"*50);
        try:
            case();
            print(f"✅ 测试通过：{name}！");
        except AssertionError as err:
            failed.append(name);
            print(f"❌ 测试失败：{name}，原因：{err}！");
    print("——"*50);
    if failed:
        print(f"❌ {len(failed)}项没过：{failed}！");
        sys.exit(1);
    print(f"✅ {len(cases)}项测试全过，核心机制都正常！");

if __name__ == "__main__":
    main();
