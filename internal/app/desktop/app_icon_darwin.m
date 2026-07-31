#import <AppKit/AppKit.h>
#import <dispatch/dispatch.h>

#include <stddef.h>

void corsaSetApplicationIcon(const unsigned char *data, size_t length) {
	if (data == NULL || length == 0) {
		return;
	}

	@autoreleasepool {
		NSData *iconData = [NSData dataWithBytes:data length:length];
		NSImage *icon = [[NSImage alloc] initWithData:iconData];
		if (icon == nil) {
			return;
		}

		dispatch_async(dispatch_get_main_queue(), ^{
			[NSApp setApplicationIconImage:icon];
			[icon release];
		});
	}
}
