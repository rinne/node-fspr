/*
 * FSPR is a collection of the interesting fs.* functionality
 * promisified.  Most of the interfaces are just simply converted by
 * replacing callbacks with promise resolve or reject. However,
 * readfile and writefile are more elaborate. In addition to static
 * buffers or strings, they can also handle input and output from
 * streams and also make optional run time constraint checks and hash
 * calculations etc.
 *
 * This is still work in progress.
 */

var fs = require('fs');

function _promisify(realFunc, realFuncArgs) {
	rv = new Promise(function(resolve, reject) {
		var cb = function() {
			var args = [];
			for (var i = 0; i < arguments.length; i++) {
				args.push(arguments[i]);
			}
			var err = args.shift();
			if (err) {
				reject(err);
			} else {
				resolve.apply(global, args);
			}
		};
		realFuncArgs.push(cb);
		try {
			realFunc.apply(global, realFuncArgs);
		} catch (err) {
			reject(err);
		}
	});
	return rv;
}

function fsprStat(path) { return _promisify(fs.stat, [ path ]); }
function fsprLStat(path) { return _promisify(fs.lstat, [ path ]); }
function fsprFStat(fd) {	return _promisify(fs.fstat, [ fd ]); }
function fsprReadLink(path) { return _promisify(fs.readlink, [ path ]); }
function fsprRename(oldPath, newPath) {	return _promisify(fs.rename, [ oldPath, newPath ]); }
function fsprRmDir(path) { return _promisify(fs.rmdir, [ path ]); }
function fsprReadDir(path) { return _promisify(fs.readdir, [ path ]); }
function fsprTruncate(path, len) { return _promisify(fs.truncate, [ path, len ]); }
function fsprUnLink(path) { return _promisify(fs.unlink, [ path ]); }
function fsprChMod(path, mode) { return _promisify(fs.chmod, [ path, mode ]); }
function fsprFChMod(fd, mode) { return _promisify(fs.fchmod, [ fd, mode ]); }
function fsprLChMod(path, mode) { return _promisify(fs.lchmod, [ path, mode ]); }
function fsprOpen(path, flags, mode) { return _promisify(fs.open, [ path, flags, mode ]); }
function fsprClose(fd) { return _promisify(fs.close, [ fd ]); }
function fsprRead(fd, buffer, offset, length, position) { return _promisify(fs.read, [ fd, buffer, offset, length, position ]); }
function fsprWrite() {
	var args = [];
	for (var i = 0; i < arguments.length; i++) {
		args.push(arguments[i]);
	}
	if ((args.length < 2) || (args.length > 5)) {
		return Promise.reject(new Error('Wrong number of arguments'));
	} 
	return _promisify(fs.write, args);
}
function fsprRealPath(path, cache) {
	return ((cache === undefined) ?
			_promisify(fs.realpath, [ path ]) :
			_promisify(fs.realpath, [ path, cache ])); }
function fsprSymLink(destination, path, type) {
	return ((type === undefined) ?
			_promisify(fs.symlink, [ destination, path ]) :
			_promisify(fs.symlink, [ destination, path, type ])); }
function fsprMkDir(path, mode) {
	return ((mode === undefined) ?
			_promisify(fs.mkdir, [ path ]) :
			_promisify(fs.mkdir, [ path, mode ])); }

function fsprReadFile(path, options) {
	options = {
		maxSize: (((typeof(options) === 'object') &&
				   (options.maxSize !== undefined)) ?
				  options.maxSize :
				  undefined),
		size: (((typeof(options) === 'object') &&
				(options.size !== undefined)) ?
			   options.size :
			   undefined),
		encoding: (((typeof(options) === 'object') &&
					(options.encoding !== undefined) &&
					(options.encoding !== 'buffer')) ?
				   options.maxSize :
				   undefined),
		outputStream: (((typeof(options) === 'object') &&
						(options.outputStream !== undefined)) ?
					   options.outputStream :
					   undefined),
		contentHash: (((typeof(options) === 'object') &&
					   (options.contentHash !== undefined)) ?
					  options.contentHash :
					  undefined),
		contentHashAlgorithm: (((typeof(options) === 'object') &&
								(options.contentHashAlgorithm !== undefined)) ?
							   options.contentHashAlgorithm :
							   undefined),
		hash: undefined
	};
	var rd = {
		op: 'read',
		path: undefined,
		size: undefined
	};
	if ((options.outputStream !== undefined) &&
		((typeof(options.outputStream) !== 'object') || (! (options.outputStream && options.outputStream.write && options.outputStream.end)))) {
		return Promise.reject(new Error('Bad outputStream'));
	}
	if (options.contentHashAlgorithm !== undefined) {
		try {
			options.hash = require('crypto').createHash(options.contentHashAlgorithm);
		} catch (err) {
			return Promise.reject(err);
		}
		rd.contentHashAlgorithm = options.contentHashAlgorithm;
		rd.contentHash = undefined;
	}
	var rv = new Promise(function(resolve, reject) {
		(fsprOpen(path, 'r')
		 .then(function(fd) {
			 var rv;
			 var off = 0;
			 var rs = fs.createReadStream(null,
										  { flags: 'r',
											encoding: null,
											fd: fd,
											mode: undefined,
											autoClose: true });
			 rd.path = path;
			 if (options.outputStream === undefined) {
				 rv = new Promise(function(resolve, reject) {
					 var buffers = [ new Buffer(0) ];
					 rs.on('data', function(data) {
						 if (rs === undefined) {
							 return;
						 }
						 if (((options.maxSize !== undefined) && ((off + data.length) > options.maxSize)) ||
							 ((options.size !== undefined) && ((off + data.length) > options.size))) {
							 rs.destroy();
							 rs = undefined;
							 fd = undefined;
							 buffers = undefined;
							 return reject(new Error('File size exceeds limit'));
						 }
						 off += data.length;
						 if (options.hash !== undefined) {
							 options.hash.update(data);
						 }
						 if (buffers[buffers.length - 1].length + data.length <= (128 * 1024)) {
							 buffers[buffers.length - 1] = Buffer.concat([buffers[buffers.length - 1], data]);
						 } else {
							 buffers.push(data);
						 }
						 if (buffers.length > 8) {
							 buffers = [ Buffer.concat(buffers) ];
						 }
					 });
					 rs.on('end', function(data) {
						 if (rs === undefined) {
							 return;
						 }
						 rs.destroy();
						 rs = undefined;
						 fd = undefined;
						 var rv = undefined;
						 switch (buffers.length) {
						 case 0:
							 rv = new Buffer(0);
							 break;
						 case 1:
							 rv = buffers[0];
							 break;
						 default:
							 rv = Buffer.concat(buffers);
						 }
						 buffers = undefined;
						 if (options.encoding !== undefined) {
							 rv = rv.toString(options.encoding);
						 }
						 rd.size = off;
						 if (options.hash !== undefined) {
							 if (options.contentHash === undefined) {
									 options.contentHash = options.hash.digest('hex');
							 } else if (options.contentHash !== options.hash.digest('hex')) {
								 return reject(new Error('File contents hash mismatch'));
							 }
							 rd.contentHash = options.contentHash;
						 }
						 return resolve(rv);
					 });
					 rs.on('error', function(err) {
						 if (rs === undefined) {
							 return;
						 }
						 rs.destroy();
						 rs = undefined;
						 fd = undefined;
						 buffers = undefined;
						 return reject(err);
					 });
				 });
			 } else {
				 rv = new Promise(function(resolve, reject) {
					 rs.on('data', function(data) {
						 if (rs === undefined) {
							 return;
						 }
						 if (((options.maxSize !== undefined) && ((off + data.length) > options.maxSize)) ||
							 ((options.size !== undefined) && ((off + data.length) > options.size))) {
							 rs.destroy();
							 rs = undefined;
							 fd = undefined;
							 return reject(new Error('File size exceeds limit'));
						 }
						 if (! options.outputStream.write(data)) {
							 if (rs !== undefined) {
								 rs.pause();
							 }
						 }
						 off += data.length;
						 if (options.hash !== undefined) {
							 options.hash.update(data);
						 }
					 });
					 rs.on('end', function(data) {
						 if (rs === undefined) {
							 return;
						 }
						 if ((options.size !== undefined) && (off != options.size)) {
							 rs.destroy();
							 rs = undefined;
							 fd = undefined;
							 return reject(new Error('File size mismatch'));
						 }
						 if (options.hash !== undefined) {
							 if (options.contentHash === undefined) {
								 options.contentHash = options.hash.digest('hex');
							 } else {
								 if ((options.contentHash !== options.hash.digest('hex'))) {
									 rs.destroy();
									 rs = undefined;
									 fd = undefined;
									 return reject(new Error('File contents hash mismatch'));
								 }
							 }
							 rd.contentHash = options.contentHash;
						 }
						 rd.size = off;
						 options.outputStream.end();
					 });
					 rs.on('error', function(err) {
						 if (rs === undefined) {
							 return;
						 }
						 rs.destroy();
						 rs = undefined;
						 fd = undefined;
						 return reject(err);
					 });
					 options.outputStream.on('finish', function() {
						 if (rs !== undefined) {
							 rs.destroy();
							 rs = undefined;
							 fd = undefined;
						 }
						 return resolve(rd);
					 });
					 options.outputStream.on('drain', function() {
						 if (rs !== undefined) {
							 rs.resume();
						 }
					 });
					 options.outputStream.on('error', function(err) {
						 if (rs !== undefined) {
							 rs.destroy();
							 rs = undefined;
							 fd = undefined;
						 }
						 return reject(err);
					 });
				 });
			 }
			 return rv;
		 })
		 .then(function(rv) {
			 return resolve(rv);
		 })
		 .catch(function(err) {
			 return reject(err);
		 }));
	});
	return rv;
}

function fsprWriteFile(path, input, options) {
	options = {
		maxSize: (((typeof(options) === 'object') &&
				   (options.maxSize !== undefined)) ?
				  options.maxSize :
				  undefined),
		size: (((typeof(options) === 'object') &&
				(options.size !== undefined)) ?
			   options.size :
			   undefined),
		encoding: (((typeof(options) === 'object') &&
					(options.encoding !== undefined) &&
					(options.encoding !== 'buffer')) ?
				   options.maxSize :
				   'utf8'),
		exclusive: (((typeof(options) === 'object') &&
					 (options.exclusive !== undefined)) ?
					(options.exclusive ? true : false) :
					false),
		append: (((typeof(options) === 'object') &&
				  (options.append !== undefined)) ?
				 (options.append ? true : false) :
				 false),
		mode: (((typeof(options) === 'object') &&
				(options.mode !== undefined)) ?
			   options.mode :
			   undefined),
		contentHash: (((typeof(options) === 'object') &&
					   (options.contentHash !== undefined)) ?
					  options.contentHash :
					  undefined),
		contentHashAlgorithm: (((typeof(options) === 'object') &&
								(options.contentHashAlgorithm !== undefined)) ?
							   options.contentHashAlgorithm :
							   undefined),
		unlinkOnError: (((typeof(options) === 'object') &&
						 (options.unlinkOnError !== undefined)) ?
						(options.unlinkOnError ? true : false) :
						false),
		fileOpened: false,
		hash: undefined,
	};
	var rd = {
		op: 'write',
		path: undefined,
		size: undefined
	};
	var inputStream;
	if (typeof (input) === 'object') {
		if (Buffer.isBuffer(input)) {
			inputStream = undefined;
		} else if (input && input.read && input.pause && input.resume) {
			inputStream = input;
			input = undefined;
		} else {
			return Promise.reject(new Error('Bad input object'));
		}
	} else {
		inputStream = undefined;
		if (typeof (input) === 'string') {
			input = new Buffer(input, options.encoding);
		} else if (typeof (input) === 'number') {
			input = new Buffer(input.toString(), options.encoding);
		} else {
			return Promise.reject(new Error('Bad input type'));
		}
	}
	if ((options.contentHash !== undefined) && (options.contentHashAlgorithm === undefined)) {
		return Promise.reject(new Error('Inconsistent contentHash and contentHashAlgorithm'));
	}
	if (options.contentHashAlgorithm !== undefined) {
		try {
			options.hash = require('crypto').createHash(options.contentHashAlgorithm);
		} catch (err) {
			return Promise.reject(err);
		}
		rd.contentHashAlgorithm = options.contentHashAlgorithm;
		rd.contentHash = undefined;
	}
	if (input !== undefined) {
		if (((options.size !== undefined) && (input.length != options.size)) ||
			((options.maxSize !== undefined) && (input.length > options.maxSize))) {
			return Promise.reject(new Error('Input size mismatch'));
		}
		if (options.hash !== undefined) {
			options.hash.update(input);
			if (options.contentHash === undefined) {
					options.contentHash = options.hash.digest('hex');
			} else if (options.contentHash !== options.hash.digest('hex')) {
				return Promise.reject(new Error('Input hash mismatch'));
			}
			rd.contentHash = options.contentHash;
			options.hash = undefined;
		}
	}
	var rv = new Promise(function(resolve, reject) {
		(fsprOpen(path,
				 ('w' + (options.exclusive ? 'x' : '') + (options.append ? '+' : '')),
				 options.mode)
		 .then(function(fd) {
			 options.fileOpened = true;
			 rd.path = path;
			 var rv;
			 if (input !== undefined) {
				 rv = new Promise(function(resolve, reject) {
					 fs.write(fd, input, 0, input.length, function(err, written, buffer) {
						 if (err) {
							 return reject(err);
						 }
						 if (written != input.length) {
							 return reject(new Error('Truncated write'));
						 }
						 rd.size = input.length;
						 return resolve(fd);
					 });
				 });
			 } else if (inputStream !== undefined) {
				 rv = new Promise(function(resolve, reject) {
					 var ws = fs.createWriteStream(null,
												   { flags: 'w',
													 fd: fd,
													 mode: undefined,
													 autoClose: true });
					 var off = 0;
					 inputStream.on('data', function(data) {
						 if (inputStream === undefined) {
							 return;
						 }
						 if (((options.size !== undefined) && ((off + data.length) > options.size)) ||
							 ((options.maxSize !== undefined) && ((off + data.length) > options.maxSize))) {
							 inputStream.destroy();
							 inputStream = undefined;
							 if (ws !== undefined) {
								 ws.destroy();
								 ws = undefined;
								 fd = undefined;
							 }
							 return reject(new Error('Input stream length mismatch'));
						 }
						 if (! ws.write(data)) {
							 inputStream.pause();
						 }
						 off += data.length;
						 if (options.hash !== undefined) {
							 options.hash.update(data);
						 }
					 });
					 inputStream.on('end', function(data) {
						 if (inputStream === undefined) {
							 return;
						 }
						 inputStream.destroy();
						 inputStream = undefined;
						 if ((options.size !== undefined) && (off != options.size)) {
							 if (ws !== undefined) {
								 ws.destroy();
								 ws = undefined;
								 fd = undefined;
							 }
							 return reject(new Error('Input stream length mismatch'));
						 }
						 rd.size = off;
						 if (options.hash !== undefined) {
							 if (options.contentHash === undefined) {
								 options.contentHash = options.hash.digest('hex');
							 } else {
								 if ((options.contentHash !== options.hash.digest('hex'))) {
									 if (ws !== undefined) {
										 ws.destroy();
										 ws = undefined;
										 fd = undefined;
									 }
									 return reject(new Error('Input stream hash mismatch'));
								 }
							 }
							 rd.contentHash = options.contentHash;
						 }
						 ws.end();
					 });
					 inputStream.on('error', function(err) {
						 if (inputStream === undefined) {
							 return;
						 }
						 if (ws !== undefined) {
							 ws.destroy();
							 ws = undefined;
							 fd = undefined;
						 }
						 inputStream.destroy();
						 inputStream = undefined;
						 return reject(err);
					 });
					 ws.on('drain', function() {
						 if (ws === undefined) {
							 return;
						 }
						 if (inputStream !== undefined) {
							 inputStream.resume();
						 }
					 });
					 ws.on('error', function(err) {
						 if (ws === undefined) {
							 return;
						 }
						 if (inputStream !== undefined) {
							 inputStream.destroy();
							 inputStream = undefined;
						 }
						 ws.destroy();
						 ws = undefined;
						 fd = undefined;
						 return reject(err);
					 });
					 ws.on('finish', function() {
						 if (ws === undefined) {
							 return;
						 }
						 if (inputStream !== undefined) {
							 inputStream.destroy();
							 inputStream = undefined;
						 }
						 ws.destroy();
						 ws = undefined;
						 fd = undefined;
						 return resolve(fd);
					 });
				 });
				 return rv;
			 } else {
				 return Promise.reject(new Error('Internal error'));
			 }
			 return rv;
		 })
		 .then(function(fd) {
			 if (fd === undefined) {
				 return Promise.resolve();
			 }
			 return fsprClose(fd);
		 })
		 .then(function() {
			 return resolve(rd);
		 })
		 .catch(function(err) {
			 var realError = err;
			 if (options.fileOpened && options.unlinkOnError) {
				 (fsprUnLink(path)
				  .then(function() {
					  return reject(realError);
				  })
				  .catch(function(err) {
					  return reject(realError);
				  }));
			 } else {
				 return reject(realError);
			 }
		 }));
	});
	return rv;
}

module.exports = {
	open: fsprOpen,
	close: fsprClose,
	mkdir: fsprMkDir,
	rmdir: fsprRmDir,
	readdir: fsprReadDir,
	readlink: fsprReadLink,
	realpath: fsprRealPath,
	rename: fsprRename,
	chmod: fsprChMod,
	fchmod: fsprFChMod,
	lchmod: fsprLChMod,
	symlink: fsprSymLink,
	unlink: fsprUnLink,
	truncate: fsprTruncate,
	stat: fsprStat,
	fstat: fsprFStat,
	lstat: fsprLStat,
	read: fsprRead,
	write: fsprWrite,
	readfile: fsprReadFile,
	writefile: fsprWriteFile
};
