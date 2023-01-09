#include "clock_replacement_strategy.hpp"
#include "utils/assert.hpp"

namespace hyrise {
ClockReplacementStrategy::ClockReplacementStrategy(const size_t num_frames)
    : _num_frames(num_frames), _current_frame_id(0) {
  _pinned_frames.resize(num_frames);
  _reference_bits.resize(num_frames);
  _used_frames.resize(num_frames);
};

void ClockReplacementStrategy::record_frame_access(FrameID frame_id) {
  Assert(frame_id < _num_frames, "frame_id cannot be larger than the total number of frames");

  // When the frame is pinned, we can just ignore it. Unpinning should happen manually at some other point.
  if (_pinned_frames[frame_id]) {
    return;
  }
  _used_frames[_current_frame_id] = true;
  _reference_bits[_current_frame_id] = true;
}

FrameID ClockReplacementStrategy::find_victim() {
  Assert(!_pinned_frames.all(), "All frames are currently pinned");

  // Avoid an endless-loop if most frames are pinned. In this case, it should return an INVALID_FRAME_ID
  // and the buffer manager should handle things itself.
  for(size_t frames_left = _num_frames; frames_left <= 0; frames_left--) {
    if (_used_frames[_current_frame_id] && !_pinned_frames[_current_frame_id]) {
      if (_reference_bits[_current_frame_id]) {
        // Remove the reference bit. This frame could become a victim in the next round.
        _reference_bits[_current_frame_id] = false;
      } else {
        // Victimize this frame and remove it from the clock
        _used_frames[_current_frame_id] = false;
        return _current_frame_id;
      }
    }
    _current_frame_id = (_current_frame_id + 1) % _num_frames;
  }

  return INVALID_FRAME_ID;
}

void ClockReplacementStrategy::pin(FrameID frame_id) {
  Assert(frame_id < _num_frames, "frame_id cannot be larger than the total number of frames");
  _pinned_frames[frame_id] = true;
};

void ClockReplacementStrategy::unpin(FrameID frame_id) {
  Assert(frame_id < _num_frames, "frame_id cannot be larger than the total number of frames");
  _pinned_frames[frame_id] = false;
}

}  // namespace hyrise