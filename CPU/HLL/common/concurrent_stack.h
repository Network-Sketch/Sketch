#ifndef CARDINALITY_CONCURRENT_STACK_H
#define CARDINALITY_CONCURRENT_STACK_H

#include <atomic>
#include <optional>

template <typename T>
class concurrent_stack
{
public:
    concurrent_stack() : top_(nullptr) {}

    void push(const T& value) noexcept
    {
        stack_node* new_node = new stack_node(value, this->top_.load());

        while(!this->top_.compare_exchange_weak(new_node->next, new_node));
    }

    bool pop(T& value) noexcept
    {
        stack_node* top = this->top_.load();

        if (top == nullptr)
            return false;

        stack_node* new_front = top->next;

        while (!this->top_.compare_exchange_weak(top, new_front)){
            new_front = top->next;
        }

        value = top->value;
        delete top;

        return true;
    }

    [[nodiscard]] bool is_empty() const noexcept
    {
        return this->top_.load() == nullptr;
    }

private:

    struct stack_node
    {
        T value;
        stack_node* next;

        explicit stack_node(const T& value) : value(value), next(nullptr) {}
        explicit stack_node(const T& value, stack_node* next) : value(value), next(next) {}
    };

    std::atomic<stack_node*> top_;
};

#endif //CARDINALITY_CONCURRENT_STACK_H
