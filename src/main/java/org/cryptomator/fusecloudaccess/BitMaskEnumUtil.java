package org.cryptomator.fusecloudaccess;

import java.util.EnumSet;
import java.util.Set;

import jnr.constants.Constant;

class BitMaskEnumUtil {

	private BitMaskEnumUtil() {
	}

	public static <E extends Enum & Constant> Set<E> bitMaskToSet(Class<E> clazz, long mask) {
		Set<E> result = EnumSet.noneOf(clazz);
		for (E e : clazz.getEnumConstants()) {
			if ((e.longValue() & mask) == e.longValue()) {
				result.add(e);
			}
		}
		return result;
	}

	public static <E extends Enum & Constant> long setToBitMask(Set<E> set) {
		long mask = 0;
		for (E value : set) {
			mask |= value.longValue();
		}
		return mask;
	}

}
