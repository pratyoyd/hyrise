#include "fsst_segment.hpp"

#include <climits>
#include <sstream>
#include <string>

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
FSSTSegment<T>::FSSTSegment(pmr_vector<pmr_string> values, pmr_vector<bool> null_values)
    : AbstractEncodedSegment{data_type_from_type<pmr_string>()},
    _null_values{std::move(null_values)} {

    // TODO (anyone): handle null values

    // our temporary data structure keeping char pointer and their length
    std::vector<unsigned long> row_lengths;
    std::vector<unsigned char*> row_pointers;
    row_lengths.reserve(values.size());
    row_pointers.reserve(values.size());

    // needed for compression
    _compressed_value_lengths.resize(values.size());
    _compressed_value_pointers.resize(values.size());

    unsigned total_length = 0;

    for (pmr_string& value : values) {
      total_length += value.size();
      row_lengths.push_back(value.size());
      row_pointers.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(value.data())));  // TODO: value.c_str()
    }

    _compressed_values.resize(16 + 2 * total_length);  // why 16? need to find out

    // create symbol table
    _encoder = fsst_create(values.size(), row_lengths.data(), row_pointers.data(), 0);

    //      unsigned char buffer[sizeof(fsst_decoder_t)];
    //      fsst_export(encoder, buffer);

    //      fsst_compress(
    //          fsst_encoder_t *encoder, /* IN: encoder obtained from fsst_create(). */
    //          size_t nstrings,         /* IN: number of strings in batch to compress. */
    //          size_t lenIn[],          /* IN: byte-lengths of the inputs */
    //          unsigned char *strIn[],  /* IN: input string start pointers. */
    //          size_t outsize,          /* IN: byte-length of output buffer. */
    //          unsigned char *output,   /* OUT: memory buffer to put the compressed strings in (one after the other). */
    //          size_t lenOut[],         /* OUT: byte-lengths of the compressed strings. */
    //          unsigned char *strOut[]  /* OUT: output string start pointers. Will all point into [output,output+size). */
    //      );

    fsst_compress(_encoder, values.size(), row_lengths.data(), row_pointers.data(), _compressed_values.size(),
                  _compressed_values.data(), _compressed_value_lengths.data(), _compressed_value_pointers.data());

    // TODO (anyone): shrink the size of _compressed_values

    // print compressed values
    for (size_t i = 0; i < _compressed_value_pointers.size(); i++) {
      for (size_t j = 0; j < _compressed_value_lengths[i]; j++) {
        printf("%d ", _compressed_value_pointers[i][j]);
      }
      printf("\n");
    }

    //      // DECOMPRESSION
    //      fsst_decoder_t decoder = fsst_decoder(encoder);
    //
    //      size_t output_size = 6 + 1;
    //      std::vector<unsigned char> output_buffer(output_size);
    //      size_t output_size_after_decompression =
    //          fsst_decompress(&decoder, compressedRowLens[0], compressedRowPtrs[0], output_size, output_buffer.data());
    //
    //      std::cout << output_size_after_decompression << std::endl;
    //
    //      for (size_t i = 0; i < output_size_after_decompression; i++) {
    //        printf("%c", output_buffer[i]);
    //      }
    //
    //      printf("\n");

      //--
   }

template <typename T>
AllTypeVariant FSSTSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  // TODO add real values
  return T{};

}

template <typename T>
std::optional<T> FSSTSegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  // TODO add real values
  return std::nullopt;
}

template <typename T>
ChunkOffset FSSTSegment<T>::size() const {
  // TODO add real values
  return static_cast<ChunkOffset>(0);
}

template <typename T>
std::shared_ptr<AbstractSegment> FSSTSegment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  // TODO add real values
  return std::shared_ptr<FSSTSegment<T>>();
}

template <typename T>
size_t FSSTSegment<T>::memory_usage(const MemoryUsageCalculationMode) const {
  // TODO add real values
  return size_t{0};
}

template <typename T>
EncodingType FSSTSegment<T>::encoding_type() const {
  // TODO add real values
  return EncodingType::FSST;
}

template <typename T>
std::optional<CompressedVectorType> FSSTSegment<T>::compressed_vector_type() const {
  // TODO add real values
  return std::nullopt;
}

template <>
std::optional<CompressedVectorType> FSSTSegment<pmr_string>::compressed_vector_type() const {
  // TODO add real values
  return std::nullopt;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(FSSTSegment);

}  // namespace opossum
