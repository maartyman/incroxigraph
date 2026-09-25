/// An iterator that can append multiple items to a caller-provided buffer.
pub trait BatchedIterator {
    type Item;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }

    /// Appends approximately `batch_size` items to `output` and returns the number appended.
    ///
    /// `batch_size` is a target rather than a hard limit. Implementations may append more items
    /// when finishing the current unit of work produces multiple results.
    fn next_batch(&mut self, output: &mut Vec<Self::Item>, batch_size: usize) -> usize;
}

impl<I: BatchedIterator + ?Sized> BatchedIterator for Box<I> {
    type Item = I::Item;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (**self).size_hint()
    }

    fn next_batch(&mut self, output: &mut Vec<Self::Item>, batch_size: usize) -> usize {
        (**self).next_batch(output, batch_size)
    }
}

/// Adapts a standard [`Iterator`] to [`BatchedIterator`].
pub struct IteratorBatched<I> {
    inner: I,
}

impl<I> IteratorBatched<I> {
    pub fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<I: Iterator> Iterator for IteratorBatched<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<I: Iterator> BatchedIterator for IteratorBatched<I> {
    type Item = I::Item;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }

    fn next_batch(&mut self, output: &mut Vec<Self::Item>, batch_size: usize) -> usize {
        let initial_len = output.len();
        output.extend(self.inner.by_ref().take(batch_size));
        output.len() - initial_len
    }
}

pub(crate) struct BatchedMap<I: BatchedIterator, F, O> {
    inner: I,
    map: F,
    is_boundary: fn(&O) -> bool,
    input: Vec<I::Item>,
}

impl<I: BatchedIterator, F, O> BatchedMap<I, F, O> {
    pub(crate) fn new(inner: I, map: F, is_boundary: fn(&O) -> bool) -> Self {
        Self {
            inner,
            map,
            is_boundary,
            input: Vec::new(),
        }
    }
}

impl<I, F, O> BatchedIterator for BatchedMap<I, F, O>
where
    I: BatchedIterator,
    F: FnMut(I::Item) -> O,
{
    type Item = O;

    fn next_batch(&mut self, output: &mut Vec<Self::Item>, batch_size: usize) -> usize {
        let initial_len = output.len();
        while output.len() - initial_len < batch_size {
            if self.input.is_empty() {
                let remaining = batch_size - (output.len() - initial_len);
                if self.inner.next_batch(&mut self.input, remaining) == 0 {
                    break;
                }
                self.input.reverse();
            }
            while let Some(item) = self.input.pop() {
                let item = (self.map)(item);
                let boundary = (self.is_boundary)(&item);
                output.push(item);
                if boundary {
                    return output.len() - initial_len;
                }
            }
        }
        output.len() - initial_len
    }
}

pub(crate) struct BatchedFilterMap<I: BatchedIterator, F, O> {
    inner: I,
    filter_map: F,
    is_boundary: fn(&O) -> bool,
    input: Vec<I::Item>,
}

impl<I: BatchedIterator, F, O> BatchedFilterMap<I, F, O> {
    pub(crate) fn new(inner: I, filter_map: F, is_boundary: fn(&O) -> bool) -> Self {
        Self {
            inner,
            filter_map,
            is_boundary,
            input: Vec::new(),
        }
    }
}

impl<I, F, O> BatchedIterator for BatchedFilterMap<I, F, O>
where
    I: BatchedIterator,
    F: FnMut(I::Item) -> Option<O>,
{
    type Item = O;

    fn next_batch(&mut self, output: &mut Vec<Self::Item>, batch_size: usize) -> usize {
        let initial_len = output.len();
        while output.len() - initial_len < batch_size {
            if self.input.is_empty() {
                let remaining = batch_size - (output.len() - initial_len);
                if self.inner.next_batch(&mut self.input, remaining) == 0 {
                    break;
                }
                self.input.reverse();
            }
            while let Some(item) = self.input.pop() {
                let Some(item) = (self.filter_map)(item) else {
                    continue;
                };
                let boundary = (self.is_boundary)(&item);
                output.push(item);
                if boundary {
                    return output.len() - initial_len;
                }
            }
        }
        output.len() - initial_len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BurstingIterator<I> {
        inner: I,
    }

    impl<I: Iterator> Iterator for BurstingIterator<I> {
        type Item = I::Item;

        fn next(&mut self) -> Option<Self::Item> {
            self.inner.next()
        }
    }

    impl<I: Iterator> BatchedIterator for BurstingIterator<I> {
        type Item = I::Item;

        fn next_batch(&mut self, output: &mut Vec<Self::Item>, _batch_size: usize) -> usize {
            let initial_len = output.len();
            output.extend(self.inner.by_ref().take(3));
            output.len() - initial_len
        }
    }

    #[test]
    fn iterator_adapter_reads_batches() {
        let mut iter = IteratorBatched::new(0..6);
        let mut output = vec![10];
        assert_eq!(iter.next_batch(&mut output, 3), 3);
        assert_eq!(output, [10, 0, 1, 2]);
        assert_eq!(iter.next_batch(&mut output, 3), 3);
        assert_eq!(output, [10, 0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn iterator_adapter_handles_empty_and_short_batches() {
        let mut iter = IteratorBatched::new([1, 2].into_iter());
        let mut output = Vec::new();
        assert_eq!(iter.next_batch(&mut output, 8), 2);
        assert_eq!(output, [1, 2]);
        assert_eq!(iter.next_batch(&mut output, 8), 0);
    }

    #[test]
    fn filter_map_preserves_a_boundary_and_buffers_later_items() {
        let inner = BurstingIterator {
            inner: [1, 0, 2].into_iter(),
        };
        let mut iter = BatchedFilterMap::new(inner, Some, |value| *value == 0);
        let mut output = Vec::new();

        assert_eq!(iter.next_batch(&mut output, 1), 2);
        assert_eq!(output, [1, 0]);
        let mut tail = Vec::new();
        assert_eq!(iter.next_batch(&mut tail, 1), 1);
        assert_eq!(tail, [2]);
        assert_eq!(iter.next_batch(&mut tail, 1), 0);
    }

    #[test]
    fn map_preserves_a_boundary_and_buffers_later_items() {
        let inner = BurstingIterator {
            inner: [1, 0, 2].into_iter(),
        };
        let mut iter = BatchedMap::new(inner, |value| value, |value| *value == 0);
        let mut output = Vec::new();

        assert_eq!(iter.next_batch(&mut output, 1), 2);
        assert_eq!(output, [1, 0]);
        let mut tail = Vec::new();
        assert_eq!(iter.next_batch(&mut tail, 1), 1);
        assert_eq!(tail, [2]);
        assert_eq!(iter.next_batch(&mut tail, 1), 0);
    }
}
