package edu.illinois.cs;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MatrixAddition {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		List<List<Integer>> first = asList(asList(1, 2), asList(3, 4));
		List<List<Integer>> second = asList(asList(1, 2), asList(3, 4));
		List<List<Integer>> result = zipWith(zipWithF(add), first, second);
		System.out.println(result);
	}

	static interface F2<A, B, C> {
		C f(A a, B b);
	}

	static final F2<Integer, Integer, Integer> add = new F2<Integer, Integer, Integer>() {
		@Override
		public Integer f(Integer a, Integer b) {
			return a + b;
		}
	};

	static <A, B, C> F2<List<A>, List<B>, List<C>> zipWithF(final F2<A, B, C> f) {
		return new F2<List<A>, List<B>, List<C>>() {
			@Override
			public List<C> f(List<A> as, List<B> bs) {
				return zipWith(f, as, bs);
			}
		};
	}

	static <A, B, C> List<C> zipWith(F2<A, B, C> f, List<A> as, List<B> bs) {
		List<C> cs = new ArrayList<C>(as.size());
		Iterator<A> ia = as.iterator();
		Iterator<B> ib = bs.iterator();
		while (ia.hasNext() && ib.hasNext())
			cs.add(f.f(ia.next(), ib.next()));
		return cs;
	}

}
