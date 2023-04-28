#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/move/utility.hpp>
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/buffer_ptr.hpp"
#include "storage/buffer/memory_resource.hpp"

#include "utils/assert.hpp"

namespace hyrise {

/**
 * The BufferPoolAllocatorObserver is used to track the allocation and deallocation of pages. A shared_ptr to the object can be registered at a 
 * BufferPoolAllocator using the register_observer method. Check AllocatorPinGuard for an example.
*/
class BufferPoolAllocatorObserver {
 public:
  virtual void on_allocate(std::shared_ptr<Frame> frame) = 0;
  virtual void on_deallocate(std::shared_ptr<Frame> frame) = 0;
  virtual ~BufferPoolAllocatorObserver() = default;
};

/**
 * The BufferPoolAllocator is a custom, polymorphic allocator that uses the BufferManager to allocate and deallocate pages.
*/
template <class T>
class BufferPoolAllocator {
 public:
  using value_type = T;
  using pointer = BufferPtr<T>;
  using const_pointer = BufferPtr<const T>;
  using void_pointer = BufferPtr<void>;
  using difference_type = typename pointer::difference_type;

  BufferPoolAllocator() : _memory_resource(&BufferManager::get_global_buffer_manager()) {}

  BufferPoolAllocator(MemoryResource* memory_resource) : _memory_resource(memory_resource) {}

  BufferPoolAllocator(boost::container::pmr::memory_resource* resource) : _memory_resource(nullptr) {
    Fail("The current BufferPoolAllocator cannot take a boost memory_resource");
  }

  BufferPoolAllocator(const BufferPoolAllocator& other) noexcept {
    _memory_resource = other.memory_resource();
    _observer = other.current_observer();
  }

  template <class U>
  BufferPoolAllocator(const BufferPoolAllocator<U>& other) noexcept {
    _memory_resource = other.memory_resource();
    _observer = other.current_observer();
  }

  BufferPoolAllocator& operator=(const BufferPoolAllocator& other) noexcept {
    _memory_resource = other.memory_resource();
    _observer = other.current_observer();
    return *this;
  }

  template <class U>
  bool operator==(const BufferPoolAllocator<U>& other) const noexcept {
    return _memory_resource == other.memory_resource() && _observer.lock() == other.current_observer().lock();
  }

  template <class U>
  bool operator!=(const BufferPoolAllocator<U>& other) const noexcept {
    return _memory_resource != other.buffer_manager() || _observer.lock() != other.current_observer().lock();
  }

  [[nodiscard]] pointer allocate(std::size_t n) {
    auto ptr = static_cast<pointer>(_memory_resource->allocate(sizeof(value_type) * n, alignof(T)));
    if (auto observer = _observer.lock()) {
      // TODO: Pass shared frame
      observer->on_allocate(ptr.get_frame(AccessIntent::Write));
    }
    return ptr;
  }

  void deallocate(pointer const ptr, std::size_t n) {
    if (auto observer = _observer.lock()) {
      // TODO: Shared frame
      observer->on_deallocate(ptr.get_frame(AccessIntent::Write));
    }
    _memory_resource->deallocate(static_cast<void_pointer>(ptr), sizeof(value_type) * n, alignof(T));
  }

  MemoryResource* memory_resource() const noexcept {
    return _memory_resource;
  }

  BufferPoolAllocator select_on_container_copy_construction() const noexcept {
    return BufferPoolAllocator(_memory_resource);
  }

  // template <typename U, class... Args>
  // void construct(const U* ptr, BOOST_FWD_REF(Args)... args) {
  //   ::new ((void*)ptr) U(boost::forward<Args>(args)...);
  // }

  // template <typename U, class Args>
  // void construct(const BufferPtr<U>& ptr, BOOST_FWD_REF(Args) args) {
  //   ::new (static_cast<void*>(ptr.operator->())) U(boost::forward<Args>(args));
  // }

  // template <class U>
  // void destroy(const BufferPtr<U>& ptr) {
  //   ptr->~U();
  // }

  void register_observer(std::shared_ptr<BufferPoolAllocatorObserver> observer) {
    if (!_observer.expired()) {
      Fail("An observer is already registered");
    }
    _observer = observer;
  }

  std::weak_ptr<BufferPoolAllocatorObserver> current_observer() const {
    return _observer;
  }

 private:
  std::weak_ptr<BufferPoolAllocatorObserver> _observer;
  MemoryResource* _memory_resource;
};

}  // namespace hyrise