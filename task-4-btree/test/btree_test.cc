#include <gtest/gtest.h>
#include <algorithm>
#include <cstddef>
#include <numeric>
#include <random>
#include <sstream>
#include <vector>
#include "moderndbs/defer.h"
#include "moderndbs/btree.h"

using BufferFrame = moderndbs::BufferFrame;
using BufferManager = moderndbs::BufferManager;
using Defer = moderndbs::Defer;
using BTree = moderndbs::BTree<uint64_t, uint64_t, std::less<uint64_t>, 1024>; // NOLINT

namespace {

// NOLINTNEXTLINE
TEST(BTreeTest, InsertEmptyTree) {
    BufferManager buffer_manager(1024, 100);
    BTree tree(0, buffer_manager);
    ASSERT_FALSE(tree.root);

    tree.insert(42, 21);

    auto test = "inserting an element into an empty B-Tree";
    ASSERT_TRUE(tree.root)
        << test << " does not create a node.";

    auto root_page = buffer_manager.fix_page(*tree.root, false);
    auto root_node = reinterpret_cast<BTree::Node*>(root_page.get_data());
    Defer root_page_unfix([&]() { buffer_manager.unfix_page(root_page, false); });

    ASSERT_TRUE(root_node->is_leaf())
        << test << " does not create a leaf node.";
    ASSERT_TRUE(root_node->count)
        << test << " does not create a leaf node with count = 1.";
}

// NOLINTNEXTLINE
TEST(BTreeTest, InsertLeafNode) {
    uint32_t page_size = 1024;
    BufferManager buffer_manager(page_size, 100);
    BTree tree(0, buffer_manager);
    ASSERT_FALSE(tree.root);

    for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
        tree.insert(i, 2 * i);
    }

    auto test = "inserting BTree::LeafNode::kCapacity elements into an empty B-Tree";
    ASSERT_TRUE(tree.root);

    auto root_page = buffer_manager.fix_page(*tree.root, false);
    auto root_node = reinterpret_cast<BTree::Node*>(root_page.get_data());
    auto root_inner_node = static_cast<BTree::InnerNode*>(root_node);
    Defer root_page_unfix([&]() { buffer_manager.unfix_page(root_page, false); });

    ASSERT_TRUE(root_node->is_leaf())
        << test << " creates an inner node as root.";
    ASSERT_EQ(root_inner_node->count, BTree::LeafNode::kCapacity)
        << test << " does not store all elements.";
}

// NOLINTNEXTLINE
TEST(BTreeTest, InsertLeafNodeSplit) {
    BufferManager buffer_manager(1024, 100);
    BTree tree(0, buffer_manager);

    ASSERT_FALSE(tree.root);

    for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
        tree.insert(i, 2 * i);
    }

    ASSERT_TRUE(tree.root);
    auto root_page = buffer_manager.fix_page(*tree.root, false);
    auto root_node = reinterpret_cast<BTree::Node*>(root_page.get_data());
    auto root_inner_node = static_cast<BTree::InnerNode*>(root_node);
    Defer root_page_unfix([&]() { buffer_manager.unfix_page(root_page, false); });
    ASSERT_TRUE(root_inner_node->is_leaf());
    ASSERT_EQ(root_inner_node->count, BTree::LeafNode::kCapacity);
    root_page_unfix.run();

    // Let there be a split...
    tree.insert(424242, 42);

    auto test = "inserting BTree::LeafNode::kCapacity + 1 elements into an empty B-Tree";

    ASSERT_TRUE(tree.root)
        << test << " removes the root :-O";

    root_page = buffer_manager.fix_page(*tree.root, false);
    root_node = reinterpret_cast<BTree::Node*>(root_page.get_data());
    root_inner_node = static_cast<BTree::InnerNode*>(root_node);
    root_page_unfix = Defer([&]() { buffer_manager.unfix_page(root_page, false); });

    ASSERT_FALSE(root_inner_node->is_leaf())
        << test << " does not create a root inner node";
    ASSERT_EQ(root_inner_node->count, 2)
        << test << " creates a new root with count != 2";
}

// NOLINTNEXTLINE
TEST(BTreeTest, LookupEmptyTree) {
    BufferManager buffer_manager(1024, 100);
    BTree tree(0, buffer_manager);

    auto test = "searching for a non-existing element in an empty B-Tree";

    ASSERT_FALSE(tree.lookup(42))
        << test << " seems to return something :-O";
}

// NOLINTNEXTLINE
TEST(BTreeTest, LookupSingleLeaf) {
    BufferManager buffer_manager(1024, 100);
    BTree tree(0, buffer_manager);

    // Fill one page
    for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
        tree.insert(i, 2 * i);
        ASSERT_TRUE(tree.lookup(i))
            << "searching for the just inserted key k=" << i << " yields nothing";
    }

    // Lookup all values
    for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
        if(i == 41){
            int stop =1;
        }
        auto v = tree.lookup(i);
        ASSERT_TRUE(v)
            << "key=" << i << " is missing";
        ASSERT_EQ(*v, 2* i)
            << "key=" << i << " should have the value v=" << 2*i;
    }
}

// NOLINTNEXTLINE
TEST(BTreeTest, LookupSingleSplit) {
    BufferManager buffer_manager(1024, 100);
    BTree tree(0, buffer_manager);

    // Insert values
    for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
        tree.insert(i, 2 * i);
    }

    tree.insert(BTree::LeafNode::kCapacity, 2 * BTree::LeafNode::kCapacity);
    ASSERT_TRUE(tree.lookup(BTree::LeafNode::kCapacity))
        << "searching for the just inserted key k=" << (BTree::LeafNode::kCapacity + 1) << " yields nothing";

    // Lookup all values
    for (auto i = 0ul; i < BTree::LeafNode::kCapacity + 1; ++i) {
        auto v = tree.lookup(i);
        ASSERT_TRUE(v)
            << "key=" << i << " is missing";
        ASSERT_EQ(*v, 2* i)
            << "key=" << i << " should have the value v=" << 2*i;
    }
}

// NOLINTNEXTLINE
TEST(BTreeTest, LookupMultipleSplitsIncreasing) {
    BufferManager buffer_manager(1024, 100);
    BTree tree(0, buffer_manager);
    auto n = 100 * BTree::LeafNode::kCapacity;

    // Insert values
    for (auto i = 0ul; i < n; ++i) {
        if(i == 1800){
            int stopHere=0;
        }
        tree.insert(i, 2 * i);
        ASSERT_TRUE(tree.lookup(i))
            << "searching for the just inserted key k=" << i << " yields nothing";
    }

    // Lookup all values
    for (auto i = 0ul; i < n; ++i) {
            if(i == 1800){
                int stopHere=0;
            }
        auto v = tree.lookup(i);
        ASSERT_TRUE(v)
            << "key=" << i << " is missing";
        ASSERT_EQ(*v, 2* i)
            << "key=" << i << " should have the value v=" << 2*i;
    }
}

// NOLINTNEXTLINE
TEST(BTreeTest, LookupMultipleSplitsDecreasing) {
    BufferManager buffer_manager(1024, 100);
    BTree tree(0, buffer_manager);
    auto n = 10 * BTree::LeafNode::kCapacity;

    // Insert values
    for (auto i = n; i > 0; --i) {
        tree.insert(i, 2 * i);
        ASSERT_TRUE(tree.lookup(i))
            << "searching for the just inserted key k=" << i << " yields nothing";
    }

    // Lookup all values
    for (auto i = n; i > 0; --i) {
        if(i == 419){
            int j=5;
        }
        auto v = tree.lookup(i);
        ASSERT_TRUE(v)
            << "key=" << i << " is missing";
        ASSERT_EQ(*v, 2* i)
            << "key=" << i << " should have the value v=" << 2*i;
    }
}

// NOLINTNEXTLINE
TEST(BTreeTest, LookupRandomNonRepeating) {
    BufferManager buffer_manager(1024, 100);
    BTree tree(0, buffer_manager);
    auto n = 10 * BTree::LeafNode::kCapacity;

    // Generate random non-repeating key sequence
    std::vector<uint64_t> keys(n);
    std::iota(keys.begin(), keys.end(), n);
    std::mt19937_64 engine(0);
    std::shuffle(keys.begin(), keys.end(), engine);

    // Insert values
    for (auto i = 0ul; i < n; ++i) {
        tree.insert(keys[i], 2 * keys[i]);
        ASSERT_TRUE(tree.lookup(keys[i]))
            << "searching for the just inserted key k=" << keys[i] << " after i=" << i << " inserts yields nothing";
    }

    // Lookup all values
    for (auto i = 0ul; i < n; ++i) {
        if(keys[i] == 589){
            int stop=9;
        }
        auto v = tree.lookup(keys[i]);
        ASSERT_TRUE(v)
            << "key=" << keys[i] << " is missing";
        ASSERT_EQ(*v, 2 * keys[i])
            << "key=" << keys[i] << " should have the value v=" << keys[i];
    }
}

// NOLINTNEXTLINE
TEST(BTreeTest, LookupRandomRepeating) {
    BufferManager buffer_manager(1024, 100);
    BTree tree(0, buffer_manager);
    auto n = 10 * BTree::LeafNode::kCapacity;

    // Insert & updated 100 keys at random
    std::mt19937_64 engine{0};
    std::uniform_int_distribution<uint64_t> key_distr(0, 99);
    std::vector<uint64_t> values(100);

    for (auto i = 1ul; i < n; ++i) {
        uint64_t rand_key = key_distr(engine);
        if(rand_key == 46){
            int stop=9;
        }
        values[rand_key] = i;
        tree.insert(rand_key, i);

        auto v = tree.lookup(rand_key);
        ASSERT_TRUE(v)
            << "searching for the just inserted key k=" << rand_key << " after i=" << (i - 1) << " inserts yields nothing";
        ASSERT_EQ(*v, i)
            << "overwriting k=" << rand_key << " with value v=" << i << " failed";
    }

    // Lookup all values
    for (auto i = 0ul; i < 100; ++i) {
        if (values[i] == 0) {
            continue;
        }
        auto v = tree.lookup(i);
        ASSERT_TRUE(v)
            << "key=" << i << " is missing";
        ASSERT_EQ(*v, values[i])
            << "key=" << i << " should have the value v=" << values[i];
    }
}

// NOLINTNEXTLINE
TEST(BTreeTest, Erase) {
    BufferManager buffer_manager(1024, 100);
    BTree tree(0, buffer_manager);

    // Insert values
    for (auto i = 0ul; i < 2 * BTree::LeafNode::kCapacity; ++i) {
        tree.insert(i, 2 * i);
    }

    // Iteratively erase all values
    for (auto i = 0ul; i < 2 * BTree::LeafNode::kCapacity; ++i) {
        ASSERT_TRUE(tree.lookup(i))
            << "k=" << i << " was not in the tree";
        tree.erase(i);
        ASSERT_FALSE(tree.lookup(i))
            << "k=" << i << " was not removed from the tree";
    }
}

}  // namespace
